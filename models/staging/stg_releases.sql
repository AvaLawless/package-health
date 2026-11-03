SELECT repo, id,
payload->>'tag_name' AS tag_name,
(payload->>'published_at')::timestamptz AS published_at,
(payload->>'prerelease')::boolean AS prerelease,
fetched_at
FROM {{ source('raw', 'raw_releases')}}
WHERE NOT (payload->>'draft')::boolean
qualify row_number() OVER (PARTITION BY repo, id ORDER BY fetched_at DESC) = 1
