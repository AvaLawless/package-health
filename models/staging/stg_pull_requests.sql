SELECT repo, id AS number,
payload->'user'->>'login' as author_login,
coalesce((payload->'user'->>'type') = 'Bot'
OR (payload->'user'->>'login') LIKE '%[bot]', FALSE) AS is_bot,
payload->>'state' AS state,
(payload->>'created_at')::timestamptz AS created_at,
(payload->>'updated_at')::timestamptz AS updated_at,
(payload->>'closed_at')::timestamptz AS closed_at,
(payload->>'merged_at')::timestamptz AS merged_at,
fetched_at

FROM {{ source('raw', 'raw_pull_requests')}}
QUALIFY row_number() OVER (PARTITION BY repo, id ORDER BY fetched_at DESC) = 1
