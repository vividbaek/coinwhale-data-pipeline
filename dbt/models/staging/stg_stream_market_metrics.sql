SELECT
    ts,
    symbol,
    last_price,
    price_change_pct_24h,
    quote_volume_24h,
    high_price_24h,
    low_price_24h,
    trade_count_24h,
    (last_price < 0 OR high_price_24h < 0 OR low_price_24h < 0) AS has_negative_price,
    quote_volume_24h < 0 AS has_negative_volume,
    trade_count_24h < 0 AS has_negative_trade_count,
    (low_price_24h > high_price_24h AND high_price_24h > 0) AS has_invalid_24h_range
FROM {{ source('stream', 'market_metrics') }}
