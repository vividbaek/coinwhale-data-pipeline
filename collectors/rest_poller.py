"""
REST API Poller: Open Interest 5초 간격 polling, L/S Ratio 5분 간격 polling.
- binance-openinterest: 미결제약정 데이터
- binance-ls-ratio: 전체 계정 롱/숏 비율
- binance-top-ls-account: 상위 트레이더 계정 기준 L/S
- binance-top-ls-position: 상위 트레이더 포지션 기준 L/S
- binance-taker-ls-ratio: 테이커 매수/매도 비율

데이터 확인용: python3 -m collectors.rest_poller
"""

import asyncio
import email.utils
import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import aiohttp

from collectors.base_collector import _dash_log
from common.bad_rows import is_json_serializable
from common.collector_health import (
    record_collector_error,
    record_process_heartbeat,
    record_source_success,
)
from common.collector_schema import validate_collector_event
from common.config import Config
from common.kafka_utils import KafkaProducerWrapper
from common.metrics import (
    collector_consecutive_errors,
    collector_errors_total,
    collector_heartbeat_timestamp,
    collector_last_error_timestamp,
    rest_backoff_seconds,
    rest_requests_total,
)

DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_MAX_BACKOFF_SECONDS = 300.0
DEFAULT_BAN_COOLDOWN_SECONDS = 900.0
DEFAULT_JITTER_RATIO = 0.2
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def parse_retry_after(header_value: str | None, *, now: datetime | None = None) -> float | None:
    if header_value is None:
        return None
    value = str(header_value).strip()
    if not value:
        return None
    try:
        seconds = float(value)
        return max(seconds, 0.0)
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(
        (parsed.astimezone(timezone.utc) - current.astimezone(timezone.utc)).total_seconds(),
        0.0,
    )


def rest_error_type_for_status(status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if status_code in {418, 403}:
        return "ban_cooldown"
    if 500 <= status_code <= 599:
        return "server_error"
    return "http_error"


def _jittered_delay(delay: float, jitter_ratio: float, random_fn: Callable[[], float]) -> float:
    ratio = max(float(jitter_ratio or 0.0), 0.0)
    if ratio <= 0 or delay <= 0:
        return max(delay, 0.0)
    factor = (1.0 - ratio) + random_fn() * ratio * 2.0
    return max(delay * factor, 0.0)


def compute_backoff_delay(
    *,
    status_code: int | None = None,
    retry_after: str | None = None,
    consecutive_failures: int = 1,
    base_delay_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    ban_cooldown_seconds: float = DEFAULT_BAN_COOLDOWN_SECONDS,
    jitter_ratio: float = DEFAULT_JITTER_RATIO,
    random_fn: Callable[[], float] = random.random,
    now: datetime | None = None,
) -> float:
    retry_after_delay = parse_retry_after(retry_after, now=now)
    max_delay = max(float(max_backoff_seconds or 0.0), 0.0)
    failure_count = max(int(consecutive_failures or 1), 1)

    if status_code == 429 and retry_after_delay is not None:
        return min(_jittered_delay(retry_after_delay, jitter_ratio, random_fn), max_delay)
    if status_code in {418, 403}:
        delay = max(float(ban_cooldown_seconds or 0.0), retry_after_delay or 0.0)
        return min(_jittered_delay(delay, jitter_ratio, random_fn), max_delay)

    delay = float(base_delay_seconds or 0.0) * (2 ** (failure_count - 1))
    if status_code == 429 and retry_after_delay is None:
        delay = max(delay, float(base_delay_seconds or 0.0))
    elif status_code is not None and 500 <= status_code <= 599:
        delay = max(delay, float(base_delay_seconds or 0.0))
    elif status_code is not None and not (500 <= status_code <= 599):
        delay = max(delay, float(base_delay_seconds or 0.0))
    return min(_jittered_delay(delay, jitter_ratio, random_fn), max_delay)


@dataclass
class RestBackoffState:
    collector: str
    symbol: str
    source: str
    base_delay_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS
    ban_cooldown_seconds: float = DEFAULT_BAN_COOLDOWN_SECONDS
    jitter_ratio: float = DEFAULT_JITTER_RATIO
    random_fn: Callable[[], float] = random.random
    consecutive_failures: int = 0
    current_backoff_seconds: float = 0.0
    last_success_timestamp: float | None = None
    last_error_timestamp: float | None = None

    def record_success(self, topic: str | None = None) -> None:
        self.consecutive_failures = 0
        self.current_backoff_seconds = 0.0
        self.last_success_timestamp = time.time()
        rest_requests_total.labels(
            collector=self.collector,
            symbol=self.symbol,
            source=self.source,
            status="success",
        ).inc()
        rest_backoff_seconds.labels(collector=self.collector, symbol=self.symbol, source=self.source).set(0.0)
        collector_consecutive_errors.labels(collector=self.collector, error_type="rest").set(0)
        record_source_success(
            collector=self.collector,
            symbol=self.symbol,
            source=self.source,
            topic=topic or "unknown",
            timestamp=self.last_success_timestamp,
        )
        collector_heartbeat_timestamp.labels(
            collector=self.collector,
            symbol=self.symbol,
            source=self.source,
            topic=topic or "unknown",
        ).set(self.last_success_timestamp)

    def record_failure(
        self,
        *,
        error_type: str,
        status_code: int | None = None,
        retry_after: str | None = None,
    ) -> float:
        self.consecutive_failures += 1
        self.last_error_timestamp = time.time()
        delay = compute_backoff_delay(
            status_code=status_code,
            retry_after=retry_after,
            consecutive_failures=self.consecutive_failures,
            base_delay_seconds=self.base_delay_seconds,
            max_backoff_seconds=self.max_backoff_seconds,
            ban_cooldown_seconds=self.ban_cooldown_seconds,
            jitter_ratio=self.jitter_ratio,
            random_fn=self.random_fn,
        )
        self.current_backoff_seconds = delay
        status = str(status_code) if status_code is not None else error_type
        rest_requests_total.labels(
            collector=self.collector,
            symbol=self.symbol,
            source=self.source,
            status=status,
        ).inc()
        rest_backoff_seconds.labels(collector=self.collector, symbol=self.symbol, source=self.source).set(delay)
        collector_errors_total.labels(collector=self.collector, error_type=error_type).inc()
        collector_last_error_timestamp.labels(collector=self.collector, error_type=error_type).set(
            self.last_error_timestamp
        )
        collector_consecutive_errors.labels(collector=self.collector, error_type="rest").set(self.consecutive_failures)
        record_collector_error(
            collector=self.collector,
            symbol=self.symbol,
            source=self.source,
            error_type=error_type,
            consecutive_errors=self.consecutive_failures,
            timestamp=self.last_error_timestamp,
        )
        return delay


def _rest_state(collector: str, symbol: str, source: str) -> RestBackoffState:
    return RestBackoffState(
        collector=collector,
        symbol=symbol,
        source=source,
        base_delay_seconds=max(_float_env("REST_BACKOFF_BASE_SECONDS", DEFAULT_BACKOFF_BASE_SECONDS), 0.1),
        max_backoff_seconds=max(_float_env("REST_MAX_BACKOFF_SECONDS", DEFAULT_MAX_BACKOFF_SECONDS), 1.0),
        ban_cooldown_seconds=max(_float_env("REST_BAN_COOLDOWN_SECONDS", DEFAULT_BAN_COOLDOWN_SECONDS), 1.0),
        jitter_ratio=max(_float_env("REST_BACKOFF_JITTER_RATIO", DEFAULT_JITTER_RATIO), 0.0),
    )


def _validate_rest_message(
    *,
    kafka: KafkaProducerWrapper,
    collector: str,
    symbol: str,
    source: str,
    topic: str,
    stream_name: str,
    payload: dict,
    ts_ms: int,
    key: str,
) -> str | None:
    message = {
        "symbol": symbol,
        "stream": stream_name,
        "data": payload,
        "ts": ts_ms,
    }
    validation = validate_collector_event(
        topic=topic,
        stream_name=stream_name,
        symbol=symbol,
        payload=payload,
        ts_ms=ts_ms,
    )
    if not validation.ok:
        error_type = validation.error_type or "schema_validation_failed"
        kafka.send_bad_row(
            original_topic=topic,
            payload=message,
            error_type=error_type,
            reason=validation.message,
            key=key,
            collector_name=collector,
            symbol=symbol,
            source=source,
        )
        return error_type
    if not is_json_serializable(message):
        error_type = "json_serialization_error"
        kafka.send_bad_row(
            original_topic=topic,
            payload=message,
            error_type=error_type,
            reason="message is not JSON serializable",
            key=key,
            collector_name=collector,
            symbol=symbol,
            source=source,
        )
        return error_type
    return None


class OpenInterestPoller:
    def __init__(self, symbol: str, interval: float = 5.0):
        self.symbol = symbol.upper()
        self.interval = interval
        self.url = f"{Config.FUTURES_REST_URL}/fapi/v1/openInterest"
        self.kafka = KafkaProducerWrapper(Config.KAFKA_BOOTSTRAP_SERVERS)
        self.running = True
        self.total_count = 0
        self.request_timeout_sec = max(
            _float_env("REST_REQUEST_TIMEOUT_SECONDS", DEFAULT_REQUEST_TIMEOUT_SECONDS),
            1.0,
        )
        self.backoff = _rest_state(self.__class__.__name__, self.symbol, "/fapi/v1/openInterest")

    async def start(self):
        _dash_log(f"🚀 OpenInterestPoller 시작 | symbol: {self.symbol} | interval: {self.interval}s")
        record_process_heartbeat(
            collector=self.__class__.__name__,
            symbol=self.symbol,
            source="/fapi/v1/openInterest",
            topic="binance-openinterest",
            running=True,
        )

        async with aiohttp.ClientSession() as session:
            while self.running:
                sleep_for = self.interval
                try:
                    async with session.get(
                        self.url,
                        params={"symbol": self.symbol},
                        timeout=aiohttp.ClientTimeout(total=self.request_timeout_sec),
                    ) as resp:
                        if resp.status == 200:
                            topic: str = "binance-openinterest"
                            ts_ms: int = int(time.time() * 1000)
                            try:
                                data = await resp.json()
                            except (
                                aiohttp.ContentTypeError,
                                json.JSONDecodeError,
                                ValueError,
                            ) as exc:
                                delay = self.backoff.record_failure(error_type="bad_response", status_code=resp.status)
                                sleep_for = max(sleep_for, delay)
                                _dash_log(
                                    f"❌ OI API bad response | path=/fapi/v1/openInterest "
                                    f"error={type(exc).__name__} backoff={delay:.1f}s"
                                )
                                data = None
                            if data is not None:
                                if not isinstance(data, dict):
                                    delay = self.backoff.record_failure(
                                        error_type="bad_response",
                                        status_code=resp.status,
                                    )
                                    sleep_for = max(sleep_for, delay)
                                    _dash_log(
                                        f"❌ OI API unexpected JSON shape | path=/fapi/v1/openInterest "
                                        f"backoff={delay:.1f}s"
                                    )
                                    data = None
                            if data is not None:
                                validation_key = self.symbol
                                validation_error = _validate_rest_message(
                                    kafka=self.kafka,
                                    collector=self.__class__.__name__,
                                    symbol=self.symbol,
                                    source="/fapi/v1/openInterest",
                                    topic=topic,
                                    stream_name="openInterest",
                                    payload=data,
                                    ts_ms=ts_ms,
                                    key=validation_key,
                                )
                                if validation_error:
                                    delay = self.backoff.record_failure(
                                        error_type=validation_error,
                                        status_code=resp.status,
                                    )
                                    sleep_for = max(sleep_for, delay)
                                    _dash_log(
                                        f"⚠️ OI bad row 격리 | path=/fapi/v1/openInterest "
                                        f"error_type={validation_error} backoff={delay:.1f}s"
                                    )
                                    data = None
                            if data is not None:
                                route = Config.build_partition_route(topic, self.symbol, ts_ms, data)
                                message = {
                                    "symbol": self.symbol,
                                    "stream": "openInterest",
                                    "data": data,
                                    "ts": ts_ms,
                                }
                                self.kafka.send(
                                    topic=topic,
                                    value=message,
                                    key=route.key,
                                    partition=route.partition,
                                    collector_name=self.__class__.__name__,
                                    symbol=self.symbol,
                                    source="/fapi/v1/openInterest",
                                )
                                self.total_count += 1

                                if self.total_count <= 3:
                                    _dash_log(f"📥 OpenInterest 수신: OI={data.get('openInterest', 'N/A')}")
                                # kafka_utils가 배치 + 시간 기반으로 자동 flush하므로 수동 flush 불필요
                                self.backoff.record_success(topic=topic)

                        else:
                            error_type = rest_error_type_for_status(resp.status)
                            delay = self.backoff.record_failure(
                                error_type=error_type,
                                status_code=resp.status,
                                retry_after=resp.headers.get("Retry-After"),
                            )
                            sleep_for = max(sleep_for, delay)
                            _dash_log(
                                f"⚠️ OI API 응답 에러 | path=/fapi/v1/openInterest "
                                f"status={resp.status} type={error_type} backoff={delay:.1f}s"
                            )

                except asyncio.TimeoutError:
                    delay = self.backoff.record_failure(error_type="network_timeout")
                    sleep_for = max(sleep_for, delay)
                    _dash_log(f"❌ OI API timeout | path=/fapi/v1/openInterest backoff={delay:.1f}s")
                except aiohttp.ClientConnectionError as e:
                    delay = self.backoff.record_failure(error_type="network_error")
                    sleep_for = max(sleep_for, delay)
                    _dash_log(
                        f"❌ OI API connection error | path=/fapi/v1/openInterest "
                        f"error={type(e).__name__} backoff={delay:.1f}s"
                    )
                except aiohttp.ClientError as e:
                    delay = self.backoff.record_failure(error_type="network_error")
                    sleep_for = max(sleep_for, delay)
                    _dash_log(
                        f"❌ OI API 요청 실패 | path=/fapi/v1/openInterest "
                        f"error={type(e).__name__} backoff={delay:.1f}s"
                    )
                except Exception as e:
                    delay = self.backoff.record_failure(error_type="unexpected_error")
                    sleep_for = max(sleep_for, delay)
                    _dash_log(f"❌ OI Poller 에러 | error={type(e).__name__} backoff={delay:.1f}s")

                await asyncio.sleep(sleep_for)

    def stop(self):
        self.running = False
        self.kafka.close()
        record_process_heartbeat(
            collector=self.__class__.__name__,
            symbol=self.symbol,
            source="/fapi/v1/openInterest",
            topic="binance-openinterest",
            running=False,
        )
        _dash_log(f"📊 OpenInterestPoller 종료 | 총 수집: {self.total_count}")


class LongShortRatioPoller:
    """
    L/S Ratio REST Poller: 4개 엔드포인트를 단일 클래스에서 처리.

    엔드포인트 / 토픽 매핑:
      globalLongShortAccountRatio → binance-ls-ratio       (전체 계정 기준)
      topLongShortAccountRatio    → binance-top-ls-account  (상위 트레이더 계정)
      topLongShortPositionRatio   → binance-top-ls-position (상위 트레이더 포지션)
      takerlongshortRatio         → binance-taker-ls-ratio  (테이커 매수/매도)

    interval: 300초 (5분) — API period=5m에 맞춤
    params: symbol=BTCUSDT, period="5m", limit=1 (최신 1건만)
    """

    # (엔드포인트 경로, stream 이름, Kafka 토픽) 튜플 목록
    _ENDPOINTS: list[tuple[str, str, str]] = [
        (
            "/futures/data/globalLongShortAccountRatio",
            "globalLongShortAccountRatio",
            "binance-ls-ratio",
        ),
        (
            "/futures/data/topLongShortAccountRatio",
            "topLongShortAccountRatio",
            "binance-top-ls-account",
        ),
        (
            "/futures/data/topLongShortPositionRatio",
            "topLongShortPositionRatio",
            "binance-top-ls-position",
        ),
        (
            "/futures/data/takerlongshortRatio",
            "takerLongShortRatio",
            "binance-taker-ls-ratio",
        ),
    ]

    def __init__(self, symbol: str, interval: float = 300.0):
        self.symbol = symbol.upper()
        self.interval = interval
        self.base_url = Config.FUTURES_REST_URL
        self.kafka = KafkaProducerWrapper(Config.KAFKA_BOOTSTRAP_SERVERS)
        self.running = True
        self.total_count = 0
        self.request_timeout_sec = max(
            _float_env("REST_REQUEST_TIMEOUT_SECONDS", DEFAULT_REQUEST_TIMEOUT_SECONDS),
            1.0,
        )
        self.backoff_by_path = {
            path: _rest_state(self.__class__.__name__, self.symbol, path) for path, _, _ in self._ENDPOINTS
        }

    async def _fetch_and_send(
        self,
        session: aiohttp.ClientSession,
        path: str,
        stream_name: str,
        topic: str,
    ) -> float:
        """단일 엔드포인트 호출 후 Kafka 전송."""
        url = f"{self.base_url}{path}"
        params = {"symbol": self.symbol, "period": "5m", "limit": 1}
        state = self.backoff_by_path[path]
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=self.request_timeout_sec),
            ) as resp:
                if resp.status == 200:
                    ts_ms: int = int(time.time() * 1000)
                    try:
                        data = await resp.json()
                    except (
                        aiohttp.ContentTypeError,
                        json.JSONDecodeError,
                        ValueError,
                    ) as exc:
                        delay = state.record_failure(error_type="bad_response", status_code=resp.status)
                        _dash_log(
                            f"❌ [{stream_name}] bad response | path={path} "
                            f"error={type(exc).__name__} backoff={delay:.1f}s"
                        )
                        return delay
                    # API는 리스트를 반환하므로 첫 번째 항목 사용
                    record = data[0] if isinstance(data, list) and data else data
                    if not isinstance(record, dict):
                        delay = state.record_failure(error_type="bad_response", status_code=resp.status)
                        _dash_log(f"❌ [{stream_name}] unexpected JSON shape | path={path} backoff={delay:.1f}s")
                        return delay
                    validation_error = _validate_rest_message(
                        kafka=self.kafka,
                        collector=self.__class__.__name__,
                        symbol=self.symbol,
                        source=path,
                        topic=topic,
                        stream_name=stream_name,
                        payload=record,
                        ts_ms=ts_ms,
                        key=self.symbol,
                    )
                    if validation_error:
                        delay = state.record_failure(error_type=validation_error, status_code=resp.status)
                        _dash_log(
                            f"⚠️ [{stream_name}] bad row 격리 | path={path} "
                            f"error_type={validation_error} backoff={delay:.1f}s"
                        )
                        return delay
                    route = Config.build_partition_route(topic, self.symbol, ts_ms, record)
                    message = {
                        "symbol": self.symbol,
                        "stream": stream_name,
                        "data": record,
                        "ts": ts_ms,
                    }
                    self.kafka.send(
                        topic=topic,
                        value=message,
                        key=route.key,
                        partition=route.partition,
                        collector_name=self.__class__.__name__,
                        symbol=self.symbol,
                        source=path,
                    )
                    self.total_count += 1

                    if self.total_count <= 4:
                        ls_ratio = record.get("longShortRatio", record.get("buySellRatio", "N/A"))
                        _dash_log(f"📥 [{stream_name}] L/S ratio={ls_ratio}")
                    state.record_success(topic=topic)
                    return 0.0
                else:
                    error_type = rest_error_type_for_status(resp.status)
                    delay = state.record_failure(
                        error_type=error_type,
                        status_code=resp.status,
                        retry_after=resp.headers.get("Retry-After"),
                    )
                    _dash_log(
                        f"⚠️ [{stream_name}] API 응답 에러 | path={path} "
                        f"status={resp.status} type={error_type} backoff={delay:.1f}s"
                    )
                    return delay
        except asyncio.TimeoutError:
            delay = state.record_failure(error_type="network_timeout")
            _dash_log(f"❌ [{stream_name}] API timeout | path={path} backoff={delay:.1f}s")
            return delay
        except aiohttp.ClientConnectionError as e:
            delay = state.record_failure(error_type="network_error")
            _dash_log(
                f"❌ [{stream_name}] API connection error | path={path} "
                f"error={type(e).__name__} backoff={delay:.1f}s"
            )
            return delay
        except aiohttp.ClientError as e:
            delay = state.record_failure(error_type="network_error")
            _dash_log(f"❌ [{stream_name}] API 요청 실패 | path={path} error={type(e).__name__} backoff={delay:.1f}s")
            return delay
        except Exception as e:
            delay = state.record_failure(error_type="unexpected_error")
            _dash_log(f"❌ [{stream_name}] Poller 에러 | error={type(e).__name__} backoff={delay:.1f}s")
            return delay

    async def start(self) -> None:
        _dash_log(f"🚀 LongShortRatioPoller 시작 | symbol: {self.symbol} | interval: {self.interval}s")
        for path, _, topic in self._ENDPOINTS:
            record_process_heartbeat(
                collector=self.__class__.__name__,
                symbol=self.symbol,
                source=path,
                topic=topic,
                running=True,
            )
        async with aiohttp.ClientSession() as session:
            while self.running:
                cooldown = 0.0
                for path, stream_name, topic in self._ENDPOINTS:
                    cooldown = max(
                        cooldown,
                        await self._fetch_and_send(session, path, stream_name, topic),
                    )
                    if cooldown > 0:
                        break
                # kafka_utils가 배치 + 시간 기반으로 자동 flush하므로 수동 flush 불필요
                await asyncio.sleep(max(self.interval, cooldown))

    def stop(self) -> None:
        self.running = False
        self.kafka.close()
        for path, _, topic in self._ENDPOINTS:
            record_process_heartbeat(
                collector=self.__class__.__name__,
                symbol=self.symbol,
                source=path,
                topic=topic,
                running=False,
            )
        _dash_log(f"📊 LongShortRatioPoller 종료 | 총 수집: {self.total_count}")


if __name__ == "__main__":
    poller = OpenInterestPoller("BTCUSDT")
    asyncio.run(poller.start())
