SELECT
    ts,
    symbol,
    futures_cvd_delta,
    futures_taker_buy_vol,
    futures_taker_sell_vol,
    futures_trade_count,
    spot_cvd_delta,
    spot_taker_buy_vol,
    spot_taker_sell_vol,
    spot_trade_count,
    whale_buy_count,
    whale_sell_count,
    whale_buy_vol,
    whale_sell_vol,
    (
        futures_taker_buy_vol < 0
        OR futures_taker_sell_vol < 0
        OR spot_taker_buy_vol < 0
        OR spot_taker_sell_vol < 0
        OR whale_buy_vol < 0
        OR whale_sell_vol < 0
    ) AS has_negative_volume,
    (futures_trade_count < 0 OR spot_trade_count < 0) AS has_negative_trade_count
FROM {{ source('stream', 'cvd') }}
