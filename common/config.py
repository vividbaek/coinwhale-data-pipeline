from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any

from common.symbol_config import get_collect_symbols, parse_symbol_list

DEFAULT_LOCAL_KAFKA_BOOTSTRAP_SERVERS: tuple[str, ...] = (
    "localhost:9092",
)
DEFAULT_DOCKER_KAFKA_BOOTSTRAP_SERVERS: tuple[str, ...] = (
    "kafka-1:29092",
)


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def unique_csv(items: list[str] | tuple[str, ...]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result


def normalize_kafka_bootstrap_servers(raw_value: str | None) -> str:
    """Return an explicit bootstrap list or the local demo broker."""
    servers = unique_csv(parse_csv(raw_value))
    if not servers:
        return ",".join(DEFAULT_LOCAL_KAFKA_BOOTSTRAP_SERVERS)

    return ",".join(servers)


@dataclass(frozen=True)
class KafkaPartitionRoute:
    """Kafka 전송 시 사용할 key/partition 계산 결과."""

    key: str
    partition: int | None = None
    shard: int | None = None
    shard_count: int = 1
    active_shard_count: int = 1
    partition_count: int = 0


class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = normalize_kafka_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    KAFKA_DEFAULT_PARTITIONS: int = int(os.getenv("KAFKA_PARTITIONS", "3"))
    KAFKA_HOT_TOPIC_PARTITIONS: int = int(os.getenv("KAFKA_HOT_TOPIC_PARTITIONS", "6"))
    # Optional active-lane cap inside each symbol's partition span.
    # Format:
    #   KAFKA_ACTIVE_LANES="binance-bookticker:ETHUSDT=4,binance-bookticker:*=2,*:BTCUSDT=3"
    # Matching priority: topic:symbol -> topic:* -> *:symbol -> *:*
    KAFKA_ACTIVE_LANES: dict[tuple[str, str], int] = {}

    # 고빈도 토픽은 실제 파티션 수를 늘리고, 심볼별로 여러 lane에 분산한다.
    HIGH_FREQUENCY_TOPICS: set[str] = {
        "binance-bookticker",
        "spot-bookticker",
        "binance-trade",
        "spot-trade",
        "binance-depth",
        "spot-depth",
    }
    SALT_TOPICS: set[str] = HIGH_FREQUENCY_TOPICS

    # 수집 대상 심볼 (환경변수 또는 config/symbols.yaml 기본값)
    # 사용법: COLLECT_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT python3 -m collectors.run_all
    COLLECT_SYMBOLS: list[str] = parse_symbol_list(
        os.getenv("COLLECT_SYMBOLS"),
        get_collect_symbols(exchange="binance", market="futures"),
    )
    # Partition routing 기준 심볼 universe. 심볼별 collector 프로세스에서도
    # 전체 universe를 공유해야 hot topic partition span이 안정적으로 나뉜다.
    ROUTING_SYMBOLS: list[str] = parse_symbol_list(
        os.getenv("ROUTING_SYMBOLS"),
        COLLECT_SYMBOLS,
    )

    # Binance WebSocket URLs
    BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/public/stream")
    FUTURES_PUBLIC_WS_URL = os.getenv(
        "BINANCE_FUTURES_PUBLIC_WS_URL",
        "wss://fstream.binance.com/public/stream?streams=",
    )
    FUTURES_MARKET_WS_URL = os.getenv(
        "BINANCE_FUTURES_MARKET_WS_URL",
        "wss://fstream.binance.com/market/stream?streams=",
    )
    FUTURES_WS_URL = os.getenv("BINANCE_FUTURES_WS_URL", FUTURES_PUBLIC_WS_URL)
    SPOT_WS_URL = os.getenv("BINANCE_SPOT_WS_URL", "wss://stream.binance.com/stream?streams=")

    FUTURES_PUBLIC_STREAMS: set[str] = {"bookTicker", "depth@100ms", "depth"}
    FUTURES_MARKET_STREAMS: set[str] = {
        "aggTrade",
        "kline_1m",
        "markPrice@1s",
        "markPrice",
        "forceOrder",
        "miniTicker",
        "ticker",
        "compositeIndex",
    }

    # Binance REST API
    FUTURES_REST_URL = "https://fapi.binance.com"

    # 토픽 매핑 (스트림 이름을 토픽명이랑 매칭)
    TOPIC_MAP = {
        "bookTicker": "binance-bookticker",
        "depth": "binance-depth",
        "trade": "binance-trade",
        "aggTrade": "binance-trade",
        "kline": "binance-kline",
        "forceOrder": "binance-liquidation",
        "markPrice": "binance-markprice",
        "openInterest": "binance-openinterest",
        "globalLongShortAccountRatio": "binance-ls-ratio",
        "topLongShortAccountRatio": "binance-top-ls-account",
        "topLongShortPositionRatio": "binance-top-ls-position",
        "takerLongShortRatio": "binance-taker-ls-ratio",
        "miniTicker": "binance-ticker",
        "ticker": "binance-ticker",
        "compositeIndex": "binance-composite-index",
    }

    # 현물 전용 토픽 매핑 (spot- prefix)
    SPOT_TOPIC_MAP = {
        "aggTrade": "spot-trade",
        "bookTicker": "spot-bookticker",
        "kline": "spot-kline",
        "depth": "spot-depth",
        "miniTicker": "spot-ticker",
        "ticker": "spot-ticker",
    }

    @classmethod
    def get_futures_ws_url_for_streams(cls, streams: list[Any]) -> str:
        stream_names = {str(getattr(stream, "value", stream)) for stream in streams}
        public_streams = stream_names & cls.FUTURES_PUBLIC_STREAMS
        market_streams = stream_names & cls.FUTURES_MARKET_STREAMS

        if public_streams and market_streams:
            raise ValueError(
                "Futures WebSocket streams must not mix /public and /market endpoints: " f"{sorted(stream_names)}"
            )
        if market_streams:
            return cls.FUTURES_MARKET_WS_URL
        return cls.FUTURES_PUBLIC_WS_URL

    @classmethod
    def get_topic(cls, stream_name: str, market_type: str = "futures") -> str:
        """스트림 이름으로 토픽명 반환"""
        topic_map = cls.SPOT_TOPIC_MAP if market_type == "spot" else cls.TOPIC_MAP
        for key, topic in topic_map.items():
            if key in stream_name:
                return topic
        return "binance-other"

    @classmethod
    def get_topic_partition_count(cls, topic: str) -> int:
        """토픽별 목표 파티션 수를 반환한다."""
        if topic in cls.HIGH_FREQUENCY_TOPICS:
            return max(cls.KAFKA_HOT_TOPIC_PARTITIONS, 1)
        return max(cls.KAFKA_DEFAULT_PARTITIONS, 1)

    @classmethod
    def get_symbol_partition_span(cls, topic: str, symbol: str) -> tuple[int, int]:
        """
        심볼이 사용할 연속 파티션 범위를 반환한다.

        예: 파티션 18개, 심볼 3개면 각 심볼은 6개씩 사용한다.
        """
        partition_count: int = cls.get_topic_partition_count(topic)
        if topic not in cls.HIGH_FREQUENCY_TOPICS or partition_count <= 1:
            return 0, partition_count

        normalized_symbol: str = symbol.upper()
        symbols: list[str] = [item.upper() for item in cls.ROUTING_SYMBOLS] or [normalized_symbol]
        if normalized_symbol not in symbols:
            symbols = [*symbols, normalized_symbol]

        symbol_index: int = symbols.index(normalized_symbol)
        symbol_count: int = len(symbols)

        if partition_count < symbol_count:
            fallback_partition: int = symbol_index % partition_count
            return fallback_partition, fallback_partition + 1

        base_span: int = partition_count // symbol_count
        extra_partitions: int = partition_count % symbol_count
        span_start: int = (base_span * symbol_index) + min(symbol_index, extra_partitions)
        span_size: int = base_span + (1 if symbol_index < extra_partitions else 0)
        return span_start, span_start + span_size

    @classmethod
    def _parse_active_lane_overrides(cls, raw_value: str | None = None) -> dict[tuple[str, str], int]:
        """Parse active lane overrides from env-style text.

        Invalid entries are ignored so a bad optional tuning value cannot stop
        the collector hot path from starting.
        """
        raw = os.getenv("KAFKA_ACTIVE_LANES", "") if raw_value is None else raw_value
        overrides: dict[tuple[str, str], int] = {}
        for item in raw.split(","):
            token = item.strip()
            if not token or "=" not in token or ":" not in token:
                continue
            left, value_raw = token.split("=", 1)
            topic_raw, symbol_raw = left.split(":", 1)
            topic = topic_raw.strip()
            symbol = symbol_raw.strip().upper()
            if not topic or not symbol:
                continue
            try:
                lane_count = int(value_raw.strip())
            except ValueError:
                continue
            if lane_count <= 0:
                continue
            overrides[(topic, symbol)] = lane_count
        return overrides

    @classmethod
    def _active_lane_overrides(cls) -> dict[tuple[str, str], int]:
        if cls.KAFKA_ACTIVE_LANES:
            return cls.KAFKA_ACTIVE_LANES
        return cls._parse_active_lane_overrides()

    @classmethod
    def get_active_lane_count(cls, topic: str, symbol: str, shard_count: int) -> int:
        """Return the number of lanes currently used inside a symbol span."""
        if shard_count <= 1:
            return 1
        normalized_symbol = symbol.upper()
        overrides = cls._active_lane_overrides()
        for key in (
            (topic, normalized_symbol),
            (topic, "*"),
            ("*", normalized_symbol),
            ("*", "*"),
        ):
            if key in overrides:
                return max(1, min(int(overrides[key]), shard_count))
        return shard_count

    @classmethod
    def _stable_hash(cls, raw_value: str) -> int:
        """Python 프로세스 재시작과 무관한 안정적인 해시값."""
        digest = hashlib.blake2b(raw_value.encode("utf-8"), digest_size=8).digest()
        return int.from_bytes(digest, byteorder="big", signed=False)

    @classmethod
    def _build_routing_seed(
        cls,
        topic: str,
        symbol: str,
        ts_ms: int,
        payload: dict[str, Any] | None = None,
    ) -> str:
        """토픽별 payload 특성을 반영해 shard seed를 만든다."""
        payload = payload or {}
        normalized_symbol: str = symbol.upper()

        if topic.endswith("trade"):
            seed_value: Any = payload.get("a") or payload.get("T") or payload.get("E") or ts_ms
        elif topic.endswith("depth"):
            seed_value = (
                payload.get("u")
                or payload.get("lastUpdateId")
                or payload.get("E")
                or f"{payload.get('b')}-{payload.get('a')}-{ts_ms}"
            )
        elif topic.endswith("bookticker"):
            seed_value = payload.get("u") or payload.get("E") or f"{payload.get('b')}-{payload.get('a')}-{ts_ms}"
        else:
            seed_value = ts_ms

        return f"{topic}|{normalized_symbol}|{seed_value}"

    @classmethod
    def build_partition_route(
        cls,
        topic: str,
        symbol: str,
        ts_ms: int,
        payload: dict[str, Any] | None = None,
    ) -> KafkaPartitionRoute:
        """
        토픽 특성에 따라 Kafka key와 explicit partition route를 계산한다.

        고빈도 토픽은 심볼별 전용 파티션 범위 안에서 deterministic subsharding을 사용한다.
        저빈도 토픽은 기존처럼 심볼 key만 사용한다.
        """
        normalized_symbol: str = symbol.upper()
        partition_count: int = cls.get_topic_partition_count(topic)

        if topic not in cls.HIGH_FREQUENCY_TOPICS or partition_count <= 1:
            return KafkaPartitionRoute(
                key=normalized_symbol,
                partition=None,
                shard=0,
                shard_count=1,
                active_shard_count=1,
                partition_count=partition_count,
            )

        span_start, span_end = cls.get_symbol_partition_span(topic, normalized_symbol)
        shard_count: int = max(span_end - span_start, 1)
        active_shard_count: int = cls.get_active_lane_count(topic, normalized_symbol, shard_count)
        routing_seed: str = cls._build_routing_seed(topic, normalized_symbol, ts_ms, payload)
        shard: int = cls._stable_hash(routing_seed) % active_shard_count
        partition: int = span_start + shard

        return KafkaPartitionRoute(
            key=f"{normalized_symbol}-lane-{shard}",
            partition=partition,
            shard=shard,
            shard_count=shard_count,
            active_shard_count=active_shard_count,
            partition_count=partition_count,
        )

    @classmethod
    def build_partition_key(
        cls,
        topic: str,
        symbol: str,
        ts_ms: int,
        payload: dict[str, Any] | None = None,
    ) -> str:
        """이전 인터페이스 호환용 key 반환 래퍼."""
        return cls.build_partition_route(topic, symbol, ts_ms, payload).key
