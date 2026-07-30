SELECT
    ts,
    symbol,
    mark_price,
    index_price,
    funding_rate,
    next_funding_time,
    mark_premium_pct,
    (mark_price < 0 OR index_price < 0) AS has_negative_price,
    abs(funding_rate) > 0.01 AS has_extreme_funding_rate,
    next_funding_time < ts AS has_stale_next_funding_time
FROM {{ source('stream', 'funding') }}
