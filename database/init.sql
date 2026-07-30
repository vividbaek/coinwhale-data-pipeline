-- Public demo schema for the CoinWhale data pipeline.
--
-- stream : Spark Structured Streaming → ClickHouse (5초 집계, PARTITION BY 월, TTL 90일)
-- hist   : 과거 데이터 적재 예시 (PARTITION BY 월, TTL 없음)
-- gold   : dbt가 소유하는 분석 view namespace


-- ============================================================
-- Database namespaces
-- ============================================================
CREATE DATABASE IF NOT EXISTS stream;
CREATE DATABASE IF NOT EXISTS hist;
CREATE DATABASE IF NOT EXISTS gold;


-- ============================================================
-- [STREAM 1] CVD + 고래 감지
-- 소스: binance-trade (선물), spot-trade (현물)
-- 집계: SUM 5초
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.cvd (
    ts                     DateTime,
    symbol                 String,
    futures_taker_buy_vol  Float64,
    futures_taker_sell_vol Float64,
    futures_cvd_delta      Float64,
    futures_trade_count    UInt32,
    spot_taker_buy_vol     Float64,
    spot_taker_sell_vol    Float64,
    spot_cvd_delta         Float64,
    spot_trade_count       UInt32,
    whale_buy_count        UInt32,
    whale_sell_count       UInt32,
    whale_buy_vol          Float64,
    whale_sell_vol         Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM 2] 강제 청산 (Liquidation Cascade)
-- 소스: binance-liquidation
-- 집계: SUM 5초
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.liquidation (
    ts               DateTime,
    symbol           String,
    liq_long_count   UInt32,
    liq_long_vol     Float64,
    liq_long_usd     Float64,
    liq_short_count  UInt32,
    liq_short_vol    Float64,
    liq_short_usd    Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM 3] 가격 스냅샷 (Basis + Spread + Funding)
-- 소스: binance-bookticker, spot-bookticker, binance-markprice
-- 집계: LAST 5초
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.price (
    ts               DateTime,
    symbol           String,
    futures_bid      Float64,
    futures_ask      Float64,
    futures_spread   Float64,
    spot_bid         Float64,
    spot_ask         Float64,
    spot_spread      Float64,
    basis_pct        Float64,
    mark_price       Float64,
    funding_rate     Float64,
    index_price      Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM 4] 미결제약정 (Open Interest)
-- 소스: binance-openinterest (REST 5초 polling)
-- 집계: LAST 5초
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.oi (
    ts              DateTime,
    symbol          String,
    open_interest   Float64,
    oi_change_pct   Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM 5] 24h 시장 요약 지표
-- 소스: binance-ticker, spot-ticker
-- 집계: LAST 5초
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.market_metrics (
    ts                   DateTime,
    symbol               String,
    price_change_pct     Float64,
    weighted_avg_price   Float64,
    last_price           Float64,
    volume_24h           Float64,
    quote_volume_24h     Float64,
    high_24h             Float64,
    low_24h              Float64,
    open_price_24h       Float64,
    composite_price      Nullable(Float64)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM 6] 마크 가격 + Funding Rate
-- 소스: binance-markprice (markPrice@1s)
-- 집계: LAST 5초
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.funding (
    ts                DateTime,
    symbol            String,
    mark_price        Float64,
    funding_rate      Float64,
    index_price       Float64,
    next_funding_time DateTime,
    mark_premium_pct  Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM 7] 롱/숏 비율
-- 소스: binance-ls-ratio, binance-top-ls-account,
--       binance-top-ls-position, binance-taker-ls-ratio (REST 5분 polling)
-- 집계: LAST 5분
-- ratio_type: globalLongShortAccountRatio | topLongShortAccountRatio |
--             topLongShortPositionRatio | takerLongShortRatio
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.ls_ratio (
    ts          DateTime,
    symbol      String,
    ratio_type  String,
    ls_ratio    Float64,
    long_ratio  Float64,
    short_ratio Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts, ratio_type)
TTL ts + INTERVAL 90 DAY;


-- ============================================================
-- [STREAM DQ] Spark micro-batch data-quality summaries
-- Invalid row samples are bounded operational evidence. Do not
-- export or commit them as source data.
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.pipeline_quality_events
(
    event_date Date DEFAULT toDate(event_time),
    event_time DateTime64(3, 'UTC') DEFAULT now64(3),
    pipeline LowCardinality(String),
    stage LowCardinality(String),
    topic LowCardinality(String),
    symbol LowCardinality(String),
    checkpoint String,
    batch_id UInt64,
    schema_version UInt16 DEFAULT 1,
    input_rows UInt64,
    parsed_rows UInt64,
    dropped_rows UInt64,
    null_critical_count UInt64,
    parse_error_count UInt64,
    event_lag_p95_ms UInt64,
    event_lag_p99_ms UInt64,
    write_rows UInt64,
    checksum String,
    error_type LowCardinality(String),
    sample_payload String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (pipeline, stage, topic, symbol, checkpoint, batch_id, event_time)
TTL event_date + INTERVAL 90 DAY;


-- ============================================================
-- [HIST] Optional historical-table examples
-- This public demo does not ship a historical loader. These tables
-- document the compatible target shape for a future replay/backfill.
-- ============================================================
CREATE TABLE IF NOT EXISTS hist.cvd (
    ts                     DateTime,
    symbol                 String,
    futures_taker_buy_vol  Float64,
    futures_taker_sell_vol Float64,
    futures_cvd_delta      Float64,
    futures_trade_count    UInt32,
    spot_taker_buy_vol     Float64,
    spot_taker_sell_vol    Float64,
    spot_cvd_delta         Float64,
    spot_trade_count       UInt32,
    whale_buy_count        UInt32,
    whale_sell_count       UInt32,
    whale_buy_vol          Float64,
    whale_sell_vol         Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS hist.liquidation (
    ts               DateTime,
    symbol           String,
    liq_long_count   UInt32,
    liq_long_vol     Float64,
    liq_long_usd     Float64,
    liq_short_count  UInt32,
    liq_short_vol    Float64,
    liq_short_usd    Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS hist.oi (
    ts              DateTime,
    symbol          String,
    open_interest   Float64,
    oi_change_pct   Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS hist.funding (
    ts                DateTime,
    symbol            String,
    mark_price        Float64,
    funding_rate      Float64,
    index_price       Float64,
    next_funding_time DateTime,
    mark_premium_pct  Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS hist.market_metrics (
    ts                   DateTime,
    symbol               String,
    price_change_pct     Float64,
    weighted_avg_price   Float64,
    last_price           Float64,
    volume_24h           Float64,
    quote_volume_24h     Float64,
    high_24h             Float64,
    low_24h              Float64,
    open_price_24h       Float64,
    composite_price      Nullable(Float64)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS hist.price (
    ts               DateTime,
    symbol           String,
    futures_bid      Float64,
    futures_ask      Float64,
    futures_spread   Float64,
    spot_bid         Float64,
    spot_ask         Float64,
    spot_spread      Float64,
    basis_pct        Float64,
    mark_price       Float64,
    funding_rate     Float64,
    index_price      Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);
