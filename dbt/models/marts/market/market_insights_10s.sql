{{ config(materialized='view', tags=['gold_core', 'serving']) }}

SELECT
    bucket_ts AS ts,
    symbol,
    sum(futures_cvd_delta) AS futures_cvd_delta,
    sum(futures_taker_buy_vol) AS futures_taker_buy_vol,
    sum(futures_taker_sell_vol) AS futures_taker_sell_vol,
    toUInt32(sum(futures_trade_count)) AS futures_trade_count,
    sum(spot_cvd_delta) AS spot_cvd_delta,
    sum(spot_taker_buy_vol) AS spot_taker_buy_vol,
    sum(spot_taker_sell_vol) AS spot_taker_sell_vol,
    toUInt32(sum(spot_trade_count)) AS spot_trade_count,
    toUInt32(sum(whale_buy_count)) AS whale_buy_count,
    toUInt32(sum(whale_sell_count)) AS whale_sell_count,
    sum(whale_buy_vol) AS whale_buy_vol,
    sum(whale_sell_vol) AS whale_sell_vol,
    toUInt32(sum(liq_long_count)) AS liq_long_count,
    sum(liq_long_vol) AS liq_long_vol,
    sum(liq_long_usd) AS liq_long_usd,
    toUInt32(sum(liq_short_count)) AS liq_short_count,
    sum(liq_short_vol) AS liq_short_vol,
    sum(liq_short_usd) AS liq_short_usd,
    argMax(futures_bid, ts) AS futures_bid,
    argMax(futures_ask, ts) AS futures_ask,
    argMax(futures_spread, ts) AS futures_spread,
    argMax(spot_bid, ts) AS spot_bid,
    argMax(spot_ask, ts) AS spot_ask,
    argMax(spot_spread, ts) AS spot_spread,
    argMax(basis_pct, ts) AS basis_pct,
    argMax(mark_price, ts) AS mark_price,
    argMax(funding_rate, ts) AS funding_rate,
    argMax(index_price, ts) AS index_price,
    argMax(open_interest, ts) AS open_interest,
    argMax(oi_change_pct, ts) AS oi_change_pct,
    argMax(price_change_pct, ts) AS price_change_pct,
    argMax(weighted_avg_price, ts) AS weighted_avg_price,
    argMax(last_price, ts) AS last_price,
    argMax(volume_24h, ts) AS volume_24h,
    argMax(quote_volume_24h, ts) AS quote_volume_24h,
    argMax(high_24h, ts) AS high_24h,
    argMax(low_24h, ts) AS low_24h,
    argMax(open_price_24h, ts) AS open_price_24h,
    argMax(composite_price, ts) AS composite_price,
    argMax(mark_premium_pct, ts) AS mark_premium_pct,
    argMax(next_funding_time, ts) AS next_funding_time
FROM
(
    SELECT
        toStartOfInterval(ts, INTERVAL 5 SECOND) AS bucket_ts,
        *
    FROM {{ ref('market_insights_5s') }}
)
GROUP BY bucket_ts, symbol
