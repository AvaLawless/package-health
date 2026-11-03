WITH run_time AS(
    SELECT timezone('UTC', max(fetched_at)) AS run_ts
    FROM {{ source('raw', 'raw_pull_requests')}}
),

repos AS (
    SELECT DISTINCT repo FROM {{ ref('stg_pull_requests')}}
),

-- every month from Jan 2024 to the run month, so empty months show 0
months AS (
    SELECT unnest(generate_series(date '2024-01-01',
    date_trunc('month', (SELECT run_ts FROM run_time))::date,
    interval 1 month))::date AS month
),

spine AS (
    SELECT r.repo, m.month FROM repos r CROSS JOIN months m
),

opened AS (
    SELECT repo, date_trunc('month', created_at)::date AS month,
    COUNT(*) AS prs_opened,
    median(hours_to_first_review) AS median_first_review_hours,
    COUNT(DISTINCT author_login) FILTER (WHERE NOT is_bot) AS active_contributors
    FROM {{ ref('fct_pull_requests')}}
    GROUP BY ALL
),

merged AS (
    SELECT repo, date_trunc('month', merged_at)::date AS month,
    COUNT(*) AS prs_merged,
    median(hours_to_merge) AS median_merge_hours
    FROM {{ ref('fct_pull_requests')}}
    WHERE is_merged
    GROUP BY ALL
),

closed AS (
    SELECT repo, date_trunc('month', closed_at)::date AS month,
    COUNT(*) AS prs_closed_unmerged
    FROM {{ref('fct_pull_requests')}}
    WHERE is_closed_unmerged
    GROUP BY ALL
),

commits AS (
    SELECT repo, date_trunc('month', committed_at)::date AS month,
    COUNT(*) AS commits
    FROM {{ref('stg_commits')}}
    GROUP BY ALL
)

SELECT
s.repo,
s.month,
coalesce(o.prs_opened, 0) AS prs_opened,
coalesce(m.prs_merged, 0) AS prs_merged,
coalesce(c.prs_closed_unmerged, 0) AS prs_closed_unmerged,
coalesce(m.prs_merged, 0)
/ nullif(coalesce(m.prs_merged, 0) + coalesce(c.prs_closed_unmerged, 0), 0) AS merge_ratio,
o.median_first_review_hours,
m.median_merge_hours,
coalesce(cm.commits, 0) AS commits,
coalesce(o.active_contributors, 0) AS active_contributors,
s.month = date_trunc('month', rt.run_ts)::date AS is_partial_month
FROM spine s
CROSS JOIN run_time rt
LEFT JOIN opened o USING (repo, month)
LEFT JOIN merged m USING (repo, month)
LEFT JOIN closed c USING (repo, month)
LEFT JOIN commits cm USING (repo, month)
