SELECT
    ts,
    symbol,
    open_interest,
    oi_change_pct,
    open_interest < 0 AS has_negative_open_interest
FROM {{ source('stream', 'oi') }}
