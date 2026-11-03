SELECT repo, id,
-- (\d+)$ takes digits at the end of a pull request number
regexp_extract(payload->>'pull_request_url', '(\d+)$', 1)::BIGINT AS pr_number,
payload->'user'->>'login' AS reviewer_login,
coalesce((payload->'user'->>'type') = 'Bot'
OR (payload->'user'->>'login') LIKE '%[bot]', FALSE) AS is_bot,
payload->>'state' AS state,
(payload->>'submitted_at')::timestamptz AS submitted_at,
fetched_at
FROM {{ source('raw', 'raw_pr_reviews')}}
WHERE (payload->>'state') <> 'PENDING'
qualify row_number() OVER (PARTITION BY repo, id ORDER BY fetched_at DESC) = 1
