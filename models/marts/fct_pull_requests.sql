WITH run_time AS (
    -- "run time" = newest fetch
    SELECT timezone('UTC', max(fetched_at)) AS run_ts
    FROM {{ source('raw', 'raw_pull_requests')}}
),

prs AS (
    SELECT * FROM {{ ref('stg_pull_requests')}}
    WHERE created_at >= '2024-01-01 00:00:00+00'::timestamptz
),

review_summary AS (
    -- only reviews by someone other than the PR author and not by bots
    SELECT
    r.repo,
    r.pr_number,
    MIN(r.submitted_at) AS first_review_at,
    bool_or(r.state = 'APPROVED') AS has_approving_review
    FROM {{ ref('stg_pr_reviews')}} r
    JOIN prs p ON p.repo = r.repo AND p.number = r.pr_number
    WHERE NOT r.is_bot
    AND r.reviewer_login IS DISTINCT FROM p.author_login
    GROUP BY r.repo, r.pr_number
)

SELECT
p.repo,
p.number,
p.author_login,
p.is_bot,
p.state,
p.created_at,
p.updated_at,
p.closed_at,
p.merged_at,
p.merged_at IS NOT NULL AS is_merged,
p.state = 'closed' AND p.merged_at IS NULL AS is_closed_unmerged,
p.state = 'open' AS is_open,
rs.first_review_at, 
date_diff('second', p.created_at, rs.first_review_at) / 3600.0 AS hours_to_first_review,
coalesce(date_diff('second', p.created_at, rs.first_review_at) / 3600.0 <= 168, FALSE) AS reviewed_within_7d,
date_diff('second', p.created_at, p.merged_at) / 3600.0 AS hours_to_merge,
date_diff('second', p.created_at, p.closed_at) / 3600.0 AS hours_to_close,
p.state = 'open' AND p.updated_at < rt.run_ts - interval 180 day AS is_stale,
coalesce(rs.has_approving_review, FALSE) AS has_approving_review

FROM prs p
-- keeps PRs that were never reviewed
LEFT JOIN review_summary rs ON rs.repo = p.repo AND rs.pr_number = p.number
CROSS JOIN run_time rt

