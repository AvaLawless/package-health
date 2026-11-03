# put your prefect flow here
from prefect import flow, task, get_run_logger
import os
import httpx
import time
from pathlib import Path
import json
import shutil
import duckdb
from datetime import datetime, timezone
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
import statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import functools
import logging
from prefect.blocks.system import Secret

# token
TOKEN = os.environ['GH_PAT'] or Secret.load("psz6ah-pat").get()

# raw directory
RAW_DIR = Path("data/raw")

# start date for fetching data
WINDOW_START = "2024-01-01T00:00:00Z"

# headers
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}

# log helper
LOGS_DIR = Path("logs")
def logs_to_file(fn):
    """Also write everything a task logs to logs/<task name>.log"""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        LOGS_DIR.mkdir(exist_ok=True)
        handler = logging.FileHandler(LOGS_DIR / f"{fn.__name__}.log")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
        task_logger = get_run_logger().logger # the logger Prefet writes task logs to
        task_logger.addHandler(handler)
        try:
            return fn(*args, **kwargs)
        finally:
            task_logger.removeHandler(handler)
            handler.close()
    return wrapper

# httpx.client
client = httpx.Client(headers=HEADERS, timeout=30.0)

# page counters for the whole run: skipped = already on disk, fetched = new API requests
PAGES_SKIPPED = 0
PAGES_FETCHED = 0
API_REQUESTS = 0

# fetch a single page helper
# makes the request, reads the rate limit headers and sleeps until the reset time if fewer than 20 requests are left.
# on a 403 or 429 error, waits for Retry-After.
# raise_for_status() raises on any 4xx or 5xx response, and Prefect retries up to 3 times with increasing delays.
# returns the full response so paginate can use response.links and response.json().
@task(retries=3, retry_delay_seconds=[10, 60, 300])
def fetch_single_page(url):
    logger = get_run_logger()
    response = client.get(url)
    global API_REQUESTS
    API_REQUESTS += 1

    # read X-RateLimit-Remaining headers on every response
    rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 100))
    rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
    logger.info(f"Rate Limit remaining: {rate_limit_remaining}")

    # check whether the rate limit is low
    if rate_limit_remaining < 20:
        logger.warning(f"Rate limit is low: {rate_limit_remaining}. Sleeping until reset at {rate_limit_reset}.")
        wait = rate_limit_reset - time.time() + 5
        time.sleep(max(wait, 0))
    if response.status_code in (403, 429):
        # waits for retry_after on 403 and 429, then raises so Prefect retries task
        retry_after = int(response.headers.get("Retry-After", 60))
        logger.warning(f"Rate limit exceeded. Sleeping for {retry_after} seconds.")
        time.sleep(retry_after)
    response.raise_for_status()
    # returns full response so pagination loop can use response.links
    return response

# helper reads a saved page; returns None if page is missing or half-written
def read_page(path):
    try: 
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None

# helper writes a page safely
def write_page(path, record):
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record))
    os.replace(tmp, path)

# pagination loop
# if a page file already exists and loads as valid JSON, the loop uses it instead of calling GitHub. A file that was half
# written when a crash hit fails to load, so that page is fetched again. A leftover .tmp file gets overwritten.
# The next URL on a skipped page: a skipped page has no response, so there's no link header to follow. The code rebuilds the
# next URL from the saved url with page=N+1.
# early stop: only matters for pulls, because only pulls are sorted by update_at and have no date filter.
def paginate(repo, endpoint, url, subfolder=None, stop_before=None):
    """land every page of url; return all records (from disk or API)."""
    global PAGES_SKIPPED, PAGES_FETCHED
    folder = RAW_DIR / repo.replace("/", "__") / (subfolder or endpoint)
    folder.mkdir(parents=True, exist_ok=True)
    records, page = [], 1
    while url:
        path = folder / f"page_{page:04d}.json"
        saved = read_page(path)
        if saved:
            # resume: page already on disk, don't spend a request to GitHub
            data = saved["data"]
            PAGES_SKIPPED += 1
            # a full page means there may be another
            next_url = (str(httpx.URL(saved["url"]).copy_set_param("page", page +1))
                        if len(data) == 100 else None)
        else:
            response = fetch_single_page(url)
            data = response.json()
            PAGES_FETCHED += 1
            write_page(path, {
                "repo": repo,
                "endpoint": endpoint,
                "url": url,
                "page": page,
                "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data": data,
            })
            next_url = response.links.get("next", {}).get("url")
        records.extend(data)
        # pulls only: sorted newest-updated first, so top once pass the cutoff
        if stop_before and data and data[-1]["updated_at"] < stop_before:
            break
        url = next_url
        page += 1
    return records

# one function per repo that calls all four endpoints
def extract_repo(repo):
    logger = get_run_logger()
    base = f"https://api.github.com/repos/{repo}"

    commits_since = get_since(repo, "commits")
    commits = paginate(repo, "commits", f"{base}/commits?since={commits_since}&per_page=100")

    pulls_since = get_since(repo, "pulls")
    pulls = paginate(repo, "pulls", f"{base}/pulls?state=all&sort=updated&direction=desc&per_page=100", 
                     stop_before=pulls_since)

    # reviews: PRs created in the window that changed since the last load
    numbers = [p["number"] for p in pulls
               if p["created_at"] >= WINDOW_START and p["updated_at"] >= pulls_since]
    reviews = 0
    for n in numbers:
        reviews += len(paginate(repo, "reviews", f"{base}/pulls/{n}/reviews?per_page=100", subfolder=f"reviews/pr_{n}"))

    releases = paginate(repo, "releases", f"{base}/releases?per_page=100")

    # record counts logged per endpoint, for each repo
    logger.info(f"{repo}: commits={len(commits)}, pulls={len(pulls)}, "
            f"reviews={reviews} (from {len(numbers)} PRs), releases={len(releases)}")

# run once at the start of extract:
def prepare_raw_dir():
    logger = get_run_logger()
    if (RAW_DIR / "_LOADED").exists():
        shutil.rmtree(RAW_DIR)
        logger.info("Found _LOADED: starting fresh from the _load_state watermark")
    elif RAW_DIR.exists():
        logger.info("No _LOADED: resuming, pages on disk will be skipped")

# reading the watermark from _load_state
def get_since(repo, endpoint):
    try:
        with duckdb.connect("packages.duckdb", read_only=True) as con:
            row = con.execute(
                "SELECT max_updated_at FROM _load_state WHERE repo = ? AND endpoint = ?", 
                [repo, endpoint]).fetchone()
    except duckdb.Error:
        row = None
    return row[0].strftime("%Y-%m-%dT%H:%M:%SZ") if row and row[0] else WINDOW_START
    
# task and flow
#REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]

@task
@logs_to_file
def extract():
    prepare_raw_dir()
    for repo in REPOS:
        extract_repo(repo)
    get_run_logger().info(f"Pages skipped (already on disk): {PAGES_SKIPPED}, "
                          f"pages fetched (API requests): {PAGES_FETCHED}")

ENDPOINT_TABLES = {
    "commits": ("raw_commits", lambda r: r["sha"], lambda r: r["commit"]["committer"]["date"]),
    "pulls": ("raw_pull_requests", lambda r: r["number"], lambda r: r["updated_at"]),
    "reviews": ("raw_pr_reviews", lambda r: r["id"], lambda r: r.get("submitted_at")),
    "releases": ("raw_releases", lambda r: r["id"], lambda r: r.get("published_at")),
}

@task
@logs_to_file
def load():
    con = duckdb.connect("packages.duckdb")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_pull_requests (
            repo VARCHAR,
            id BIGINT,
            updated_at TIMESTAMP,
            fetched_at TIMESTAMP,
            source_file VARCHAR,
            payload JSON,
            PRIMARY KEY (repo, id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_commits (
            repo VARCHAR,
            id VARCHAR,
            updated_at TIMESTAMP,
            fetched_at TIMESTAMP,
            source_file VARCHAR,
            payload JSON,
            PRIMARY KEY (repo, id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_pr_reviews (
            repo VARCHAR,
            id BIGINT,
            updated_at TIMESTAMP,
            fetched_at TIMESTAMP,
            source_file VARCHAR,
            payload JSON,
            PRIMARY KEY (repo, id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_releases (
            repo VARCHAR,
            id BIGINT,
            updated_at TIMESTAMP,
            fetched_at TIMESTAMP,
            source_file VARCHAR,
            payload JSON,
            PRIMARY KEY (repo, id)
        )
    """)
    for endpoint, (table, get_id, get_date) in ENDPOINT_TABLES.items():
        for path in RAW_DIR.glob(f"*/{endpoint}/**/page_*.json"):
            page = json.loads(path.read_text())
            rows = [(page["repo"], get_id(r), get_date(r), page["fetched_at"], str(path), json.dumps(r))
                    for r in page["data"]]
            if rows:
                con.executemany(f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?, ?)", rows)

    # create a _load_state table
    # max_updated_at = the newest updated_at loaded for that repo and endpoint
    con.execute("""
        CREATE TABLE IF NOT EXISTS _load_state (
            repo VARCHAR,
            endpoint VARCHAR,
            max_updated_at TIMESTAMP,
            loaded_at TIMESTAMP,
            PRIMARY KEY (repo, endpoint))
            """)

    # fill _load_state table
    # for each raw table, this finds the newest updated_at per repo and saves it under the endpoint name
    for endpoint, (table, _, _) in ENDPOINT_TABLES.items():
        con.execute(f"""
            INSERT OR REPLACE INTO _load_state
            SELECT repo, ?, max(updated_at), current_timestamp
            FROM {table}
            GROUP BY repo
        """, [endpoint])
    
    # row counts for idempotency check
    logger = get_run_logger()
    for table, _, _ in ENDPOINT_TABLES.values():
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        logger.info(f"{table}: {count} rows")

    con.close()
    (RAW_DIR / "_LOADED").touch()

# Resolve relative to this file
DBT_DIR = Path(__file__).parent

@task(log_prints=True)
@logs_to_file
def dbt_build():
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=str(DBT_DIR),
            profiles_dir=str(DBT_DIR),
        )
    )
    return runner.invoke(["build"])


# raw table -> (staging model, filter staging ap
STAGING_FILTERS = {
    "raw_commits":       ("stg_commits",      "true"), 
    "raw_pull_requests": ("stg_pull_requests", "true"),
    "raw_pr_reviews":    ("stg_pr_reviews",    "(payload->>'state') <> 'PENDING'"),
    "raw_releases":      ("stg_releases",      "not (payload->>'draft')::boolean"),
}

# complete months with no commits that were conf loss)
KNOWN_GAPS = {("dask/dask", "2026-10-01")}

def show(con, sql):
    """Run a query and return it as a printable table."""
    return "\n" + str(con.sql(sql))

# Part 1
def check_completeness(con, logger):
    problems = []

    # 1. every (repo, id) in data/raw/ is in its raw table
    for endpoint, (table, get_id, _) in ENDPOINT_TABLES.items():
        file_keys = set()
        for path in RAW_DIR.glob(f"*/{endpoint}/**/page_*.json"):
            page = json.loads(path.read_text())
            file_keys |= {(page["repo"], str(get_id(r))) for r in page["data"]}
        table_keys = set(con.execute(f"select repo, id::varchar from {table}").fetchall())
        missing = file_keys - table_keys
        logger.info(f"{table}: {len(file_keys)} records on disk, {len(missing)} missing from table")
        if missing:
            problems.append(f"{table}: {len(missing)} records on disk not loaded, e.g. {sorted(missing)[:3]}")

    # 2. raw row count matches staging (after the same filter staging applies)
    for table, (model, where) in STAGING_FILTERS.items():
        raw_n = con.execute(f"select count(*) from {table} where {where}").fetchall()[0][0]
        stg_n = con.execute(f"select count(*) from {model}").fetchall()[0][0]
        logger.info(f"{table} -> {model}: {raw_n} raw rows, {stg_n} staging rows")
        if raw_n != stg_n:
            problems.append(f"{model}: {stg_n} rows but {table} has {raw_n}")

    # 3. commits in every complete month
    gaps = con.execute("""
        select repo, strftime(month, '%Y-%m-%d')
        from mart_repo_monthly
        where not is_partial_month and commits = 0
        order by repo, month
    """).fetchall()
    for repo, month in gaps:
        if (repo, month) in KNOWN_GAPS:
            logger.warning(f"{repo} has 0 commits in {month[:7]} (known gap, confirmed on GitHub)")
        else:
            problems.append(f"{repo} has 0 commits in complete month {month[:7]}")

    if problems:
        raise ValueError("Completeness checks failed:\n" + "\n".join(problems))
    logger.info("Completeness checks passed")

# Part 2
def detect_outliers(con, logger):
    # cutoff per repo: Q3 + 3 * IQR (values far above the typical range)
    logger.info("Outlier rule: value > Q3 + 3 * (Q3 - Q1), computed per repo")
    merge_outliers = """
        with merged as (
            select repo, number, hours_to_merge
            from fct_pull_requests where is_merged
        ),
        cutoffs as (
            select repo,
                   quantile_cont(hours_to_merge, 0.75)
                     + 3 * (quantile_cont(hours_to_merge, 0.75) - quantile_cont(hours_to_merge, 0.25)) as cutoff
            from merged group by repo
        )
        select m.repo, m.number, round(m.hours_to_merge / 24, 1) as days_to_merge,
               round(c.cutoff / 24, 1) as cutoff_days
        from merged m join cutoffs c using (repo)
        where m.hours_to_merge > c.cutoff
    """
    logger.info("Time-to-merge outliers per repo:" + show(con, f"""
        select repo, count(*) as flagged, any_value(cutoff_days) as cutoff_days, max(days_to_merge) as max_days
        from ({merge_outliers}) group by repo order by repo"""))
    logger.info("Top 5 slowest merges per repo:" + show(con, f"""
        select * from ({merge_outliers})
        qualify row_number() over (partition by repo order by days_to_merge desc) <= 5
        order by repo, days_to_merge desc"""))
    logger.info("Weekly commit outliers:" + show(con, """
        with weekly as (
            select repo, date_trunc('week', committed_at)::date as week, count(*) as commits
            from stg_commits
            where committed_at >= '2024-01-01'::timestamptz
            group by all
        ),
        cutoffs as (
            select repo,
                   quantile_cont(commits, 0.75)
                     + 3 * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) as cutoff
            from weekly group by repo
        )
        select w.repo, w.week, w.commits, c.cutoff
        from weekly w join cutoffs c using (repo)
        where w.commits > c.cutoff
        order by w.repo, w.week"""))

    logger.info("Decision: KEEP all flagged records. They are real events (long-running PRs, "
                "release bursts), and every scored metric uses medians or shares, which a few "
                "extreme values barely move.")

# Part 3
def describe(con, logger):
    logger.info("Descriptive statistics per repo (hours):" + show(con, """
        select repo,
               round(median(hours_to_first_review), 1)             as median_first_review_h,
               round(quantile_cont(hours_to_first_review, 0.9), 1) as p90_first_review_h,
               round(median(hours_to_merge), 1)                    as median_merge_h,
               round(quantile_cont(hours_to_merge, 0.9), 1)        as p90_merge_h
        from fct_pull_requests
        group by repo order by repo"""))
    logger.info("Activity and release cadence per repo:" + show(con, """
        select repo,
               round(commits_per_week, 1)  as commits_wk,
               round(releases_per_year, 1) as releases_yr
        from mart_health_inputs order by repo"""))


@task
@logs_to_file
def validate():
    logger = get_run_logger()
    con = duckdb.connect("packages.duckdb")
    con.execute("SET TimeZone = 'UTC'").fetchall()
    try:
        check_completeness(con, logger)
        detect_outliers(con, logger)
        describe(con, logger)
    finally:
        con.close()

# column in mart_health_inputs -> (dimension, +1 if higher is healthier, -1 if lower is healthier, weight within dimension)
METRICS = {
    "commits_per_week": ("activity", +1, 0.3),
    "commit_trend": ("activity", +1, 0.7),
    "merge_rate": ("throughput", +1, 0.5),
    "median_hours_to_merge": ("throughput", -1, 0.5),
    "pct_open_stale": ("backlog", -1, 1.0),
}

# throughput has the highest weight because merging contributions are a strong metric of showing a project is maintained.
DIMENSION_WEIGHTS = {"activity": 0.2, "throughput": 0.5, "backlog": 0.3}

def zscores(values):
    """Standardize a {repo: value} dict to mean 0, standard deviation 1"""
    mean = statistics.mean(values.values())
    sd = statistics.pstdev(values.values())
    return {repo: (v - mean) / sd if sd else 0.0 for repo, v in values.items()}

def score_repos(con, logger):
    cols = ", ".join(METRICS)
    rows = con.execute(f"select repo, {cols} from mart_health_inputs").fetchall()
    data = {row[0]: dict(zip(METRICS, row[1:])) for row in rows}
    repos = list(data)

    # 1. standardize each metric, flipping sign so higher always means healthier for consistency
    z = {m: zscores({r: data[r][m] for r in repos}) for m in METRICS}
    for m, (_, direction, _) in METRICS.items():
        z[m] = {r: direction * v for r, v in z[m].items()}

    # 2. dimensions subscore = weighted average of its metrics' z-scores
    sub = {r: {} for r in repos}
    for dim in DIMENSION_WEIGHTS:
        dim_metrics = {m: w for m, (d, _, w) in METRICS.items() if d == dim}
        total_w = sum(dim_metrics.values())
        for r in repos:
            sub[r][dim] = sum(z[m][r] * w for m, w in dim_metrics.items()) / total_w

    # 3. overall score = weighted average of dimension subscores, then rank
    score = {r: sum(sub[r][d] * w for d, w in DIMENSION_WEIGHTS.items()) for r in repos}
    ranked = sorted(repos, key=score.get, reverse=True)

    results = [(rank, r, round(score[r], 3), *(round(sub[r][d], 3) for d in DIMENSION_WEIGHTS))
               for rank, r in enumerate(ranked, start=1)]

    header = f"{'rank':<5}{'repo':<28}{'score':>8}" + "".join(f"{d:>12}" for d in DIMENSION_WEIGHTS)
    lines = [header] + ["f{rk:<5}{r:<28}{s:>8}" + "".join(f"{v:>12}" for v in subs)
                        for rk, r, s, *subs in results]
    logger.info("Health ranking:\n" + "\n.".join(lines))
    return results

def save_scores(results, logger):
    dims = list(DIMENSION_WEIGHTS)
    with duckdb.connect("packages.duckdb") as con:
        con.execute(f"""
            create or replace table health_scores (
                rank integer, repo varchar, score double,
                {", ".join(f"{d}_score double" for d in dims)})
            """).fetchall()
        con.executemany(f"insert into health_scores values ({', '.join(['?'] * (3 + len(dims)))})", results)
    logger.info(f"Wrote {len(results)} rows to health scores")

@task
@logs_to_file
def model_and_rank():
    logger = get_run_logger()
    with duckdb.connect("packages.duckdb") as con:
        results = score_repos(con, logger)
    save_scores(results, logger)


# plots
PLOTS_DIR = Path("plots")

# one fixed color + marker per repo, used in every plot
REPO_STYLE = {
    "dask/dask":                 {"color": "#2a78d6", "marker": "o"},
    "plotly/plotly.py":          {"color": "#eb6834", "marker": "s"},
    "scikit-learn/scikit-learn": {"color": "#1baf7a", "marker": "^"},
}

plt.rcParams.update({
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.edgecolor": "#8a8a85", "axes.labelcolor": "#3d3d3a",
    "xtick.color": "#5c5c58", "ytick.color": "#5c5c58",
    "axes.grid": True, "grid.color": "#e6e5e0", "grid.linewidth": 0.8,
    "axes.axisbelow": True, "font.size": 10, "axes.titlesize": 11,
    "figure.facecolor": "white", "savefig.dpi": 150,
})


def plot_dimension_profile(con):
    rows = con.execute(""" 
        select rank, repo, score, activity_score, throughput_score, backlog_score
        from health_scores order by rank
    """).fetchall()
    panels = [("Overall score", 2), ("Activity", 3), ("Throughput", 4), ("Backlog", 5)]

    fig, axes = plt.subplots(1, 4, figsize=(13, 4), sharey=True)
    labels = [f"#{r[0]} {r[1].split('/')[1]}\n({r[2]:+.2f})" for r in rows]
    for ax, (title, col) in zip(axes, panels):
        values = [r[col] for r in rows]
        colors = [REPO_STYLE[r[1]]["color"] for r in rows]
        ax.barh(labels, values, color=colors, height=0.6)
        ax.axvline(0, color="#3d3d3a", linewidth=1)
        ax.set_title(title)
        ax.set_xlabel("z-score (0 = average of the 3 repos)")
        ax.grid(axis="y", visible=False)
        for y, v in enumerate(values):
            ax.annotate(f"{v:+.2f}", (v, y), xytext=(4 if v >= 0 else -4, 0),
                        textcoords="offset points", va="center",
                        ha="left" if v >= 0 else "right", fontsize=9, color="#3d3d3a")
        lim = max(abs(v) for v in values) * 1.45
        ax.set_xlim(-lim, lim)
    axes[0].invert_yaxis()   # rank 1 on top
    fig.suptitle("Health ranking by dimension (repos ordered healthiest to least healthy; higher = healthier)",
                 fontsize=12)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "dimension_profile.png")
    plt.close(fig)
        
        
def plot_monthly_trends(con):
    rows = con.execute("""
        select repo, month, commits, prs_opened, prs_merged, merge_ratio,
               median_first_review_hours, active_contributors
        from mart_repo_monthly
        where not is_partial_month
        order by repo, month
    """).fetchall()
    panels = [
        (2, "Commits", "commits per month"),
        (3, "PRs opened", "PRs per month"),
        (4, "PRs merged", "PRs per month"),
        (5, "Merge ratio", "merged / (merged + closed unmerged)"),
        (6, "Median time to first review", "hours (log scale)"),
        (7, "Active PR authors", "distinct non-bot authors"),
    ]
               
    fig, axes = plt.subplots(2, 3, figsize=(15, 8), sharex=True)
    for ax, (col, title, ylabel) in zip(axes.flat, panels):
        for repo, style in REPO_STYLE.items():
            pts = [(r[1], r[col]) for r in rows if r[0] == repo and r[col] is not None]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=style["color"],
                    linewidth=2, label=repo)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        if col == 6:
            ax.set_yscale("log")
        if col == 5:
            ax.set_ylim(0, 1.05)
        ax.tick_params(axis="x", rotation=30)
    for ax in axes[1]:
        ax.set_xlabel("month")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.95))
    fig.suptitle("Monthly activity, January 2024 to last complete month", fontsize=13, y=0.99)
    fig.tight_layout(rect=(0, 0, 1, 0.92))  
    fig.savefig(PLOTS_DIR / "monthly_trends.png")
    plt.close(fig)
        
            
def plot_review_vs_merge(con):
    rows = con.execute("""
        select repo, median_first_review_hours, merge_ratio, active_contributors
        from mart_repo_monthly
        where not is_partial_month
          and median_first_review_hours is not null
          and merge_ratio is not null
    """).fetchall()
    
    fig, ax = plt.subplots(figsize=(9, 6))
    for repo, style in REPO_STYLE.items():
        pts = [r for r in rows if r[0] == repo]
        ax.scatter([p[1] for p in pts], [p[2] for p in pts],
                   s=[max(p[3], 1) * 6 for p in pts],
                   color=style["color"], marker=style["marker"], alpha=0.6,
                   edgecolors="white", linewidths=1, label=repo)
    ax.set_xscale("log")
    ax.set_xlabel("median hours to first review in the month (log scale)")
    ax.set_ylabel("merge ratio: merged / (merged + closed unmerged)")
    ax.set_ylim(0, 1.05)
    ax.set_title("Faster review vs. merge ratio, one point per repo-month\n"
                 "(marker size = active PR authors that month)")
    legend = ax.legend(frameon=False, loc="lower left", markerscale=0.6)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "review_vs_merge.png")
    plt.close(fig)
                   
                   
def make_plots(logger):
    PLOTS_DIR.mkdir(exist_ok=True)
    with duckdb.connect("packages.duckdb") as con:
        plot_dimension_profile(con)
        plot_monthly_trends(con)
        plot_review_vs_merge(con)
    logger.info(f"Saved 3 plots to {PLOTS_DIR}/")

@task
@logs_to_file
def plot():
    make_plots(get_run_logger())

@flow(name="psz6ah-package-health")
def package_health():
    extract()
    load()
    dbt_build()
    validate()
    model_and_rank()
    plot()

if __name__ == "__main__":
    package_health()