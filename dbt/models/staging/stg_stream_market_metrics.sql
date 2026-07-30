SELECT
    ts,
    symbol,
    last_price,
    price_change_pct,
    weighted_avg_price,
    volume_24h,
    quote_volume_24h,
    high_24h,
    low_24h,
    open_price_24h,
    composite_price,
    (last_price < 0 OR high_24h < 0 OR low_24h < 0) AS has_negative_price,
    (volume_24h < 0 OR quote_volume_24h < 0) AS has_negative_volume,
    (low_24h > high_24h AND high_24h > 0) AS has_invalid_24h_range
FROM {{ source('stream', 'market_metrics') }}
