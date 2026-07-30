{{ config(materialized='view', tags=['gold_core', 'serving']) }}

SELECT
    c.ts AS ts,
    c.symbol AS symbol,
    c.futures_cvd_delta,
    c.futures_taker_buy_vol,
    c.futures_taker_sell_vol,
    c.futures_trade_count,
    c.spot_cvd_delta,
    c.spot_taker_buy_vol,
    c.spot_taker_sell_vol,
    c.spot_trade_count,
    c.whale_buy_count,
    c.whale_sell_count,
    c.whale_buy_vol,
    c.whale_sell_vol,
    l.liq_long_count,
    l.liq_long_vol,
    l.liq_long_usd,
    l.liq_short_count,
    l.liq_short_vol,
    l.liq_short_usd,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.futures_bid, 0.0) AS futures_bid,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.futures_ask, 0.0) AS futures_ask,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.futures_spread, 0.0) AS futures_spread,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.spot_bid, 0.0) AS spot_bid,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.spot_ask, 0.0) AS spot_ask,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.spot_spread, 0.0) AS spot_spread,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.basis_pct, 0.0) AS basis_pct,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.mark_price, 0.0) AS mark_price,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.funding_rate, 0.0) AS funding_rate,
    if(dateDiff('second', p.ts, c.ts) <= 60, p.index_price, 0.0) AS index_price,
    if(dateDiff('second', o.ts, c.ts) <= 120, o.open_interest, 0.0) AS open_interest,
    if(dateDiff('second', o.ts, c.ts) <= 120, o.oi_change_pct, 0.0) AS oi_change_pct,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.price_change_pct, 0.0) AS price_change_pct,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.weighted_avg_price, 0.0) AS weighted_avg_price,
    if(
        dateDiff('second', m.ts, c.ts) <= 60 AND m.last_price > 0,
        m.last_price,
        if(dateDiff('second', p.ts, c.ts) <= 60, p.mark_price, 0.0)
    ) AS last_price,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.volume_24h, 0.0) AS volume_24h,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.quote_volume_24h, 0.0) AS quote_volume_24h,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.high_24h, 0.0) AS high_24h,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.low_24h, 0.0) AS low_24h,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.open_price_24h, 0.0) AS open_price_24h,
    if(dateDiff('second', m.ts, c.ts) <= 60, m.composite_price, NULL) AS composite_price,
    if(dateDiff('second', f.ts, c.ts) <= 60, f.mark_premium_pct, 0.0) AS mark_premium_pct,
    if(dateDiff('second', f.ts, c.ts) <= 60, f.next_funding_time, toDateTime(0)) AS next_funding_time
FROM {{ source('stream', 'cvd') }} AS c FINAL
LEFT JOIN {{ source('stream', 'liquidation') }} AS l FINAL
    ON c.ts = l.ts AND c.symbol = l.symbol
LEFT ASOF JOIN {{ source('stream', 'price') }} AS p FINAL
    ON c.symbol = p.symbol AND c.ts >= p.ts
LEFT ASOF JOIN {{ source('stream', 'oi') }} AS o FINAL
    ON c.symbol = o.symbol AND c.ts >= o.ts
LEFT ASOF JOIN {{ source('stream', 'market_metrics') }} AS m FINAL
    ON c.symbol = m.symbol AND c.ts >= m.ts
LEFT ASOF JOIN {{ source('stream', 'funding') }} AS f FINAL
    ON c.symbol = f.symbol AND c.ts >= f.ts
