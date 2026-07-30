SELECT
    ts,
    symbol,
    futures_bid,
    futures_ask,
    spot_bid,
    spot_ask,
    mark_price,
    index_price,
    funding_rate,
    basis_pct,
    futures_spread,
    spot_spread,
    (futures_bid < 0 OR futures_ask < 0 OR spot_bid < 0 OR spot_ask < 0 OR mark_price < 0) AS has_negative_price,
    (futures_bid > futures_ask AND futures_ask > 0) AS has_crossed_futures_book,
    (spot_bid > spot_ask AND spot_ask > 0) AS has_crossed_spot_book,
    abs(funding_rate) > 0.01 AS has_extreme_funding_rate
FROM {{ source('stream', 'price') }}
