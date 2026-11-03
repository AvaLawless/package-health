SELECT repo, id AS sha,
payload->'author'->>'login' AS author_login,
coalesce((payload->'author'->>'type') = 'Bot'
OR (payload->'author'->>'login') LIKE '%[bot]', FALSE) AS is_bot,
(payload->'commit'->'author'->>'date')::timestamptz AS committed_at,
fetched_at
FROM {{ source('raw', 'raw_commits')}}
qualify row_number() OVER (PARTITION BY repo, id ORDER BY fetched_at DESC) = 1
