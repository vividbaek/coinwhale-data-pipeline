# spark_jobs/kafka_reader.py
"""
사실상 kafka의 함수 모음 집(데이터 읽고 불러오는 용도)
단독으로 실행할 때는 depth 토픽을 1초마다 콘솔에 찍는 테스트/확인용
stream_preprocess.py 같은 전처리 job이 여기서 create, read, parse등 import해서 사용
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, lit, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

_ENVELOPE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("data", StructType([]), True),  # placeholder; per-topic schemas below
        StructField("ts", LongType(), True),
        StructField("schema_version", StringType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("ingested_at_ms", LongType(), True),
        StructField("source", StringType(), True),
        StructField("collector", StringType(), True),
        StructField("topic", StringType(), True),
    ]
)

_BOOKTICKER_DATA_SCHEMA = StructType(
    [
        StructField("b", StringType(), True),  # best bid
        StructField("a", StringType(), True),  # best ask
    ]
)

_BOOKTICKER_ROLLUP_DATA_SCHEMA = StructType(
    [
        StructField("b", DoubleType(), True),
        StructField("a", DoubleType(), True),
        StructField("bucket_ms", LongType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("message_count", LongType(), True),
    ]
)

_TRADE_CVD_ROLLUP_DATA_SCHEMA = StructType(
    [
        StructField("bucket_ms", LongType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("buy_qty", DoubleType(), True),
        StructField("sell_qty", DoubleType(), True),
        StructField("trade_count", LongType(), True),
        StructField("whale_buy_count", LongType(), True),
        StructField("whale_sell_count", LongType(), True),
        StructField("whale_buy_qty", DoubleType(), True),
        StructField("whale_sell_qty", DoubleType(), True),
    ]
)

_MARKPRICE_DATA_SCHEMA = StructType(
    [
        StructField("p", StringType(), True),  # mark price
        StructField("r", StringType(), True),  # funding rate
        StructField("i", StringType(), True),  # index price
        StructField("T", LongType(), True),  # next funding time (ms)
        StructField("E", LongType(), True),  # event time (ms)
    ]
)

_TRADE_DATA_SCHEMA = StructType(
    [
        StructField("p", StringType(), True),  # price
        StructField("q", StringType(), True),  # quantity
        StructField("T", LongType(), True),  # trade time (ms)
        StructField("m", StringType(), True),  # is buyer maker (boolean-like)
    ]
)

_TICKER_DATA_SCHEMA = StructType(
    [
        StructField("P", StringType(), True),
        StructField("w", StringType(), True),
        StructField("c", StringType(), True),
        StructField("v", StringType(), True),
        StructField("q", StringType(), True),
        StructField("h", StringType(), True),
        StructField("l", StringType(), True),
        StructField("o", StringType(), True),
        StructField("E", LongType(), True),
    ]
)

_OI_DATA_SCHEMA = StructType(
    [
        StructField("openInterest", StringType(), True),
        StructField("time", LongType(), True),
    ]
)


def _parse_with_schema(df, data_schema: StructType):
    """
    Kafka value(JSON string)를 from_json(schema)로 파싱.
    get_json_object 대비 CPU 사용량을 크게 줄인다.
    """
    schema = StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("stream", StringType(), True),
            StructField("data", data_schema, True),
            StructField("ts", LongType(), True),
            StructField("schema_version", StringType(), True),
            StructField("event_time_ms", LongType(), True),
            StructField("ingested_at_ms", LongType(), True),
            StructField("source", StringType(), True),
            StructField("collector", StringType(), True),
            StructField("topic", StringType(), True),
        ]
    )
    v = col("value").cast("string")
    return df.withColumn("_raw_value", v).withColumn("_j", from_json(v, schema))


def _quality_metadata_columns():
    j = col("_j")
    return [
        col("_j").alias("_j"),
        col("_raw_value"),
        get_json_object(col("_raw_value"), "$.schema_version").cast("string").alias("_schema_version"),
        when(j.isNull(), lit("parse_error")).otherwise(lit("ok")).alias("_parse_status"),
        when(j.isNull(), lit("malformed_json")).otherwise(lit(None).cast("string")).alias("_parse_error"),
    ]


# 스파크 작업을 시작하기 위한 "환경 설정"
def create_spark_session(app_name="BinanceProcessor"):
    """
    Spark 세션 생성 (재사용 가능)
    appname은 토픽임
    spark.jars.pacages: 카프카 전용 라이브러리 불러옴
    checkpointLocation: 스트리밍 중 에러 났을 때, 기록하는 것
    log4j2.properties: 필요한 로그만 보려고 정리함.
    """
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
    spark_version = os.getenv("SPARK_VERSION", "3.5.8")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
        )
        .config(
            "spark.sql.streaming.checkpointLocation",
            f"/opt/spark/work-dir/checkpoints/{app_name}",
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.configurationFile=file:/opt/spark/work-dir/log4j2.properties",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dlog4j.configurationFile=file:/opt/spark/work-dir/log4j2.properties",
        )
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_from_kafka(spark, topic, starting_offsets="latest", max_offsets_per_trigger=None, group_id=None):
    """
    Kafka에서 데이터 읽기 (재사용 가능)
    kafka.bootstrap.servers: kafka29092라는 주소로 접속
    subscribe: 인자로 받은 topic 구독
    startingOffsets: latest는 지금부터, earlist는 과거 데이터부터 다 가져오겠다는 것
    failOnDataLoss: 데이터가 일부 없어도 멈추지 말고 계속 진행(안전장치)
    maxOffsetsPerTrigger: 한번에 너무 많이 가져오면 렉 걸리니 1,000개씩으로 제한
                        None이면 자동으로 Lag에 따라 조정 (기본값: 1000, Lag > 1M이면 10000)
    group_id: Kafka consumer group ID (안정적인 이름 사용 → kafka-exporter 모니터링용)
              None이면 Spark 기본 랜덤 UUID 사용
    """
    # max_offsets_per_trigger가 지정되지 않았으면 기본값 사용
    if max_offsets_per_trigger is None:
        max_offsets_per_trigger = "1000"

    reader = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092"),
        )
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.request.timeout.ms", "90000")
        .option("kafka.max.poll.interval.ms", "300000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .option("kafka.metadata.max.age.ms", "300000")
        .option("kafka.reconnect.backoff.ms", "50")
        .option("kafka.reconnect.backoff.max.ms", "1000")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    )

    if group_id:
        reader = reader.option("kafka.group.id", group_id)

    return reader.load()


###### 카프카에서 온 데이터는 value라는 컬럼 안에 모든 내용이 JSON 문자열로 있음 (파싱해야함) ####
def parse_depth_data(df):
    """
    Depth 데이터 파싱 (재사용 가능)
    바이낸스의 Depth 데이터에서 매수/매도 1호가 가격(bid_price, ask_price)만 가져옴
    $data,b[0][0] JSON 구조 안에서 위치를 찾아감감
    """
    return df.select(
        get_json_object(col("value").cast("string"), "$.symbol").alias("symbol"),
        get_json_object(col("value").cast("string"), "$.data.b[0][0]").cast("double").alias("bid_price"),
        get_json_object(col("value").cast("string"), "$.data.a[0][0]").cast("double").alias("ask_price"),
        col("timestamp").alias("kafka_timestamp"),
    )


def parse_trade_data(df):
    """
    aggTrade 데이터 파싱 (재사용 가능). 체결가/수량/시각 추출.
    실시간 체결 내역(agggTrade)처리
    바이낸스에서는 시간을 밀리초(ms)로 줌. 윈도우 집계를 위해 1,000우로 나눠처 초로 변경
    .catst(string) 카프카는 데이터를 효율적으로 보관하기 위해 이진수 형태로 저장(binary -> string으로 변경)
    alias는 별칭
    """
    parsed = _parse_with_schema(df, _TRADE_DATA_SCHEMA)
    j = col("_j")
    d = col("_j.data")
    return parsed.select(
        j["symbol"].alias("symbol"),
        d["p"].cast("double").alias("price"),
        d["q"].cast("double").alias("quantity"),
        (d["T"].cast("long") / 1000).alias("event_time_sec"),
        d["m"].cast("boolean").alias("is_buyer_maker"),
        *_quality_metadata_columns(),
    )


def parse_spot_trade_data(df):
    """
    현물 aggTrade 데이터 파싱.
    선물 parse_trade_data와 동일한 구조이나, source='spot' 컬럼 추가.
    CVD 계산 시 선물/현물 구분용.
    """
    parsed = _parse_with_schema(df, _TRADE_DATA_SCHEMA)
    j = col("_j")
    d = col("_j.data")
    return parsed.select(
        j["symbol"].alias("symbol"),
        d["p"].cast("double").alias("price"),
        d["q"].cast("double").alias("quantity"),
        (d["T"].cast("long") / 1000).alias("event_time_sec"),
        d["m"].cast("boolean").alias("is_buyer_maker"),
        *_quality_metadata_columns(),
    )


def parse_trade_cvd_rollup_data(df):
    """Parse rollup-trade-cvd-1s messages produced by processors/trade_cvd_rollup.py."""
    schema = StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("stream", StringType(), True),
            StructField("market", StringType(), True),
            StructField("data", _TRADE_CVD_ROLLUP_DATA_SCHEMA, True),
            StructField("ts", LongType(), True),
        ]
    )
    parsed = df.withColumn("_raw_value", col("value").cast("string")).withColumn(
        "_j", from_json(col("value").cast("string"), schema)
    )
    j = col("_j")
    d = col("_j.data")
    is_futures = j["market"] == "futures"
    is_spot = j["market"] == "spot"
    return parsed.select(
        j["symbol"].alias("symbol"),
        j["market"].alias("source"),
        (d["bucket_ms"].cast("long") / 1000).alias("event_time_sec"),
        when(is_futures, d["buy_qty"].cast("double")).otherwise(0.0).alias("futures_taker_buy_qty"),
        when(is_futures, d["sell_qty"].cast("double")).otherwise(0.0).alias("futures_taker_sell_qty"),
        when(is_futures, d["trade_count"].cast("long")).otherwise(0).alias("futures_trade_count"),
        when(is_spot, d["buy_qty"].cast("double")).otherwise(0.0).alias("spot_taker_buy_qty"),
        when(is_spot, d["sell_qty"].cast("double")).otherwise(0.0).alias("spot_taker_sell_qty"),
        when(is_spot, d["trade_count"].cast("long")).otherwise(0).alias("spot_trade_count"),
        d["whale_buy_count"].cast("long").alias("whale_buy_count"),
        d["whale_sell_count"].cast("long").alias("whale_sell_count"),
        d["whale_buy_qty"].cast("double").alias("whale_buy_qty"),
        d["whale_sell_qty"].cast("double").alias("whale_sell_qty"),
        *_quality_metadata_columns(),
    )


def parse_kline_data(df):
    """Kline(1분봉) 데이터 파싱. Binance가 이미 1분 집계한 값."""
    # kline은 중첩 schema가 깊어 여기서는 기존 구현 유지 (핫패스 아님)

    v = col("value").cast("string")
    return df.select(
        get_json_object(v, "$.symbol").alias("symbol"),
        (get_json_object(v, "$.data.k.t").cast("long") / 1000).alias("window_start_sec"),
        get_json_object(v, "$.data.k.o").cast("double").alias("open"),
        get_json_object(v, "$.data.k.h").cast("double").alias("high"),
        get_json_object(v, "$.data.k.l").cast("double").alias("low"),
        get_json_object(v, "$.data.k.c").cast("double").alias("close"),
        get_json_object(v, "$.data.k.v").cast("double").alias("volume"),
        get_json_object(v, "$.data.k.n").cast("long").alias("trades"),
        get_json_object(v, "$.data.k.x").cast("boolean").alias("is_candle_closed"),
    )


def parse_liquidation_data(df):
    """
    forceOrder(강제 청산) 데이터 파싱.
    side=SELL → 롱 청산(하락 압력), side=BUY → 숏 청산(상승 압력).
    """
    v = col("value").cast("string")
    return df.select(
        get_json_object(v, "$.symbol").alias("symbol"),
        get_json_object(v, "$.data.o.S").alias("side"),
        get_json_object(v, "$.data.o.p").cast("double").alias("price"),
        get_json_object(v, "$.data.o.q").cast("double").alias("quantity"),
        (get_json_object(v, "$.data.o.T").cast("long") / 1000).alias("event_time_sec"),
    )


def parse_markprice_data(df):
    """
    markPrice@1s 데이터 파싱.
    마크 가격, 펀딩레이트, 인덱스 가격, 다음 펀딩 정산 시각 추출.
    """
    parsed = _parse_with_schema(df, _MARKPRICE_DATA_SCHEMA)
    j = col("_j")
    d = col("_j.data")
    return parsed.select(
        j["symbol"].alias("symbol"),
        d["p"].cast("double").alias("mark_price"),
        d["r"].cast("double").alias("funding_rate"),
        d["i"].cast("double").alias("index_price"),
        (d["T"].cast("long") / 1000).alias("next_funding_time_sec"),
        (d["E"].cast("long") / 1000).alias("event_time_sec"),
        *_quality_metadata_columns(),
    )


def parse_ticker_data(df):
    """
    miniTicker/ticker 데이터 파싱.
    24h 시장 요약 지표: 변동률, 가중평균가, 최근가, 거래량, 거래대금, 고/저/시가.
    """
    parsed = _parse_with_schema(df, _TICKER_DATA_SCHEMA)
    j = col("_j")
    d = col("_j.data")
    return parsed.select(
        j["symbol"].alias("symbol"),
        d["P"].cast("double").alias("price_change_pct"),
        d["w"].cast("double").alias("weighted_avg_price"),
        d["c"].cast("double").alias("last_price"),
        d["v"].cast("double").alias("volume_24h"),
        d["q"].cast("double").alias("quote_volume_24h"),
        d["h"].cast("double").alias("high_24h"),
        d["l"].cast("double").alias("low_24h"),
        d["o"].cast("double").alias("open_price_24h"),
        (d["E"].cast("long") / 1000).alias("event_time_sec"),
        *_quality_metadata_columns(),
    )


def parse_bookticker_data(df):
    """
    bookTicker 데이터 파싱 (선물/현물 공통).
    best bid/ask 가격 추출. insight_price 집계에 사용.
    $.data.b: best bid price, $.data.a: best ask price
    """
    parsed = _parse_with_schema(df, _BOOKTICKER_DATA_SCHEMA)
    j = col("_j")
    d = col("_j.data")
    # event time은 payload의 ts(collector가 찍은 ms)를 사용
    return parsed.select(
        j["symbol"].alias("symbol"),
        d["b"].cast("double").alias("bid_price"),
        d["a"].cast("double").alias("ask_price"),
        (j["ts"].cast("long") / 1000).alias("event_time_sec"),
        *_quality_metadata_columns(),
    )


def parse_bookticker_rollup_data(df):
    """Parse rollup-bookticker-1s messages produced by processors/bookticker_rollup.py."""
    schema = StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("stream", StringType(), True),
            StructField("market", StringType(), True),
            StructField("data", _BOOKTICKER_ROLLUP_DATA_SCHEMA, True),
            StructField("ts", LongType(), True),
        ]
    )
    parsed = df.withColumn("_raw_value", col("value").cast("string")).withColumn(
        "_j", from_json(col("value").cast("string"), schema)
    )
    j = col("_j")
    d = col("_j.data")
    return parsed.select(
        j["symbol"].alias("symbol"),
        j["market"].alias("market"),
        d["b"].cast("double").alias("bid_price"),
        d["a"].cast("double").alias("ask_price"),
        (d["event_time_ms"].cast("long") / 1000).alias("event_time_sec"),
        d["message_count"].cast("long").alias("message_count"),
        *_quality_metadata_columns(),
    )


def parse_oi_data(df):
    """
    Open Interest 데이터 파싱.
    REST polling으로 수집된 미결제약정 데이터. insight_oi 집계에 사용.
    $.data.openInterest: 미결제약정 수량 (BTC 단위)
    $.data.time: 기준 시각 (ms)
    """
    parsed = _parse_with_schema(df, _OI_DATA_SCHEMA)
    j = col("_j")
    d = col("_j.data")
    return parsed.select(
        j["symbol"].alias("symbol"),
        d["openInterest"].cast("double").alias("open_interest"),
        (d["time"].cast("long") / 1000).alias("event_time_sec"),
        *_quality_metadata_columns(),
    )


def parse_ls_ratio_data(df):
    """
    롱/숏 비율 데이터 파싱 (4개 토픽 공통).

    globalLongShortAccountRatio / topLongShortAccountRatio / topLongShortPositionRatio:
      data.longShortRatio, data.longAccount, data.shortAccount, data.timestamp
    takerlongshortRatio:
      data.buySellRatio, data.buyVol, data.sellVol, data.timestamp
      (taker는 long_ratio/short_ratio = 0.0, ls_ratio = buySellRatio)
    ratio_type: $.stream 값 (globalLongShortAccountRatio / takerLongShortRatio 등)
    """
    # LS ratio는 엔드포인트별 data schema가 달라서 기존 구현 유지
    from pyspark.sql.functions import coalesce, get_json_object, lit

    v = col("value").cast("string")
    return df.select(
        get_json_object(v, "$.symbol").alias("symbol"),
        get_json_object(v, "$.stream").alias("ratio_type"),
        coalesce(
            get_json_object(v, "$.data.longShortRatio").cast("double"),
            get_json_object(v, "$.data.buySellRatio").cast("double"),
            lit(0.0),
        ).alias("ls_ratio"),
        coalesce(
            get_json_object(v, "$.data.longAccount").cast("double"),
            lit(0.0),
        ).alias("long_ratio"),
        coalesce(
            get_json_object(v, "$.data.shortAccount").cast("double"),
            lit(0.0),
        ).alias("short_ratio"),
        (get_json_object(v, "$.data.timestamp").cast("long") / 1000).alias("event_time_sec"),
    )


def main():
    import time

    spark = create_spark_session("BinanceDepthReader")

    # Kafka는 이미 실행 중. Consumer 연결 전 짧은 대기 (coordinator 대비)
    print("⏳ Kafka 연결 전 대기 (5초)...")
    time.sleep(5)

    # Kafka 읽기 (earliest로 변경하여 기존 데이터도 읽기)
    print("📥 Kafka에서 데이터 읽기 시작...")
    kafka_df = read_from_kafka(spark, "binance-depth", starting_offsets="earliest")

    # 파싱
    parsed_df = parse_depth_data(kafka_df)

    # 출력 (1초마다 배치 처리)
    print("🚀 스트리밍 쿼리 시작... (1초마다 배치 처리)")
    query = parsed_df.writeStream.outputMode("append").format("console").trigger(processingTime="1 second").start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
