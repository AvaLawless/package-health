WITH all_releases AS (
    -- compute the gap over ALL releases, before filtering to the window, 
    -- so the first 2024 release still measures back to its predecessor
    SELECT
    repo,
    id,
    tag_name,
    published_at,
    prerelease,
    date_diff('day',
                -- gets previous release's date within the same repo; partition by repo stops from comparing a dask
                -- release from a plotly one
                lag(published_at) OVER (PARTITION BY repo ORDER BY published_at),
                published_at) AS days_since_previous
    FROM {{ ref('stg_releases')}}
    WHERE published_at IS NOT NULL
)

SELECT
    repo,
    id,
    tag_name,
    published_at,
    prerelease AS is_prerelease,
    days_since_previous
FROM all_releases
WHERE published_at >= '2024-01-01 00:00:00+00'::timestamptz