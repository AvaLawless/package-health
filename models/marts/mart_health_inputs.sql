WITH run_time AS (
    SELECT timezone('UTC', max(fetched_at)) AS run_ts
    FROM {{ source('raw', 'raw_pull_requests')}} 
),

window_len AS (
    SELECT run_ts, date_diff('day', '2024-01-01'::timestamptz, run_ts) AS days
    FROM run_time 
),

commit_stats AS (
    SELECT repo, COUNT(*) AS total_commits
    FROM {{ ref('stg_commits')}}
    WHERE committed_at >= '2024-01-01'::timestamptz
    GROUP BY repo
),

-- slope of monthly commits over complete months 
trend AS (
    SELECT repo,
    regr_slope(commits, date_diff('month', date '2024-01-01', month)) AS commit_trend
    FROM {{ ref('mart_repo_monthly')}}
    WHERE NOT is_partial_month
    GROUP BY repo
),

pr_stats AS (
    SELECT p.repo,
    avg(reviewed_within_7d::int)
    FILTER (WHERE created_at <= w.run_ts - interval 7 day) AS pct_reviewed_within_7d,
    median(hours_to_first_review) AS median_hours_to_first_review,
    median(hours_to_merge) AS median_hours_to_merge,
    SUM(is_merged::int)
    / nullif(sum(is_merged::int) + SUM(is_closed_unmerged::int), 0) AS merge_rate,
    avg(is_stale::int) FILTER (WHERE is_open) AS pct_open_stale
    FROM {{ref('fct_pull_requests')}} p
    CROSS JOIN window_len w
    GROUP BY p.repo
),

release_stats AS (
    SELECT repo, COUNT(*) AS releases
    FROM {{ ref('fct_releases')}}
    GROUP BY repo
)

SELECT
p.repo,
cs.total_commits / (w.days / 7.0) AS commits_per_week,
t.commit_trend,
p.pct_reviewed_within_7d,
p.median_hours_to_first_review,
p.median_hours_to_merge,
p.merge_rate,
p.pct_open_stale,
coalesce(r.releases, 0) / (w.days / 365.25) AS releases_per_year
FROM pr_stats p
CROSS JOIN window_len w
LEFT JOIN commit_stats cs USING (repo)
LEFT JOIN trend t USING (repo)
LEFT JOIN release_stats r USING (repo)