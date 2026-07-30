# collectors/base_collector.py
import asyncio
import json
import os
import random
import sys
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime

try:
    import websockets
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency in lean test envs
    websockets = None

from common.bad_rows import is_json_serializable
from common.collector_health import (
    record_collector_error,
    record_collector_reconnect,
    record_process_heartbeat,
    record_source_success,
    reset_collector_error,
)
from common.collector_schema import (
    DEFAULT_SCHEMA_VERSION,
    infer_event_time_ms,
    validate_collector_event,
)
from common.config import Config
from common.kafka_utils import KafkaProducerWrapper
from common.metrics import (
    collector_errors_total,
    collector_last_message_timestamp,
    collector_tps,
    kafka_messages_produced_total,
    kafka_partition_key_bucket_total,
    websocket_reconnects_total,
)
from utils.binance_stream_enum import BinanceStreamType

# ── 대시보드 출력 전략 ──────────────────────────────────────────────────
# TTY(터미널 직접 실행): ANSI 커서 제어로 제자리 업데이트
# non-TTY(PM2 등):       _dash_update skip, _dash_log만 일반 print
#   → WebSocket 수신 TPS/누적은 Prometheus에서 조회 (curl localhost:8889/metrics | grep tps)
# ─────────────────────────────────────────────────────────────────────

_IS_TTY = sys.stdout.isatty()
_dash_lock = threading.Lock()
_dash_entries = []  # [[name, status_text], ...]


def _dash_register(name: str) -> int:
    """이 collector 전용 상태 줄 예약 후 인덱스 반환 (TTY 전용)"""
    with _dash_lock:
        idx = len(_dash_entries)
        _dash_entries.append([name, "대기 중..."])
        if _IS_TTY:
            sys.stdout.write("\n")  # 전용 줄 확보
            sys.stdout.flush()
        return idx


def _dash_update(idx: int, text: str):
    """TTY 전용: idx번 상태 줄만 제자리 업데이트. non-TTY에서는 skip."""
    if not _IS_TTY:
        return
    with _dash_lock:
        _dash_entries[idx][1] = text
        n = len(_dash_entries)
        up = n - idx
        sys.stdout.write(f"\033[{up}F{text}\033[K\033[{up}E")
        sys.stdout.flush()


def _dash_log(message: str):
    """이벤트 로그 출력. TTY: ANSI 삽입 / non-TTY: 일반 print."""
    with _dash_lock:
        if _IS_TTY:
            n = len(_dash_entries)
            if n > 0:
                sys.stdout.write(f"\033[{n}F\033[L{message}\033[K\033[{n+1}E")
            else:
                sys.stdout.write(f"{message}\n")
            sys.stdout.flush()
        else:
            print(message, flush=True)


# ──────────────────────────────────────────────────────────────────────


class BaseBinanceCollector(ABC):
    def __init__(
        self,
        symbol: str,
        streams: list,
        base_url: str = None,
        market_type: str = "futures",
    ):
        self.symbol = symbol.lower()
        self.streams = streams
        self.market_type = market_type
        if base_url is not None:
            self.base_url = base_url
        elif market_type == "spot":
            self.base_url = Config.SPOT_WS_URL
        else:
            self.base_url = Config.get_futures_ws_url_for_streams(streams)
        self.url = f"{self.base_url}{'/'.join([f'{self.symbol}@{s.value if isinstance(s, BinanceStreamType) else s}' for s in streams])}"

        # 메트릭 관리
        self.total_count = 0
        self.sec_count = 0
        self.start_time = None
        self.last_report_time = None
        self.printed_samples = 0
        self.running = True
        self.kafka = KafkaProducerWrapper(Config.KAFKA_BOOTSTRAP_SERVERS)
        self._startup_verify_messages = max(
            int(os.getenv("KAFKA_STARTUP_VERIFY_MESSAGES", "5")),
            0,
        )
        self._ws_recv_timeout_sec = max(
            float(os.getenv("WS_RECV_TIMEOUT_SEC", "1.0")),
            0.1,
        )
        self._ws_idle_reconnect_sec = max(
            float(os.getenv("WS_IDLE_RECONNECT_SEC", "15.0")),
            self._ws_recv_timeout_sec,
        )
        self._ws_ping_interval_sec = max(
            float(os.getenv("WS_PING_INTERVAL_SEC", "20.0")),
            1.0,
        )
        self._ws_ping_timeout_sec = max(
            float(os.getenv("WS_PING_TIMEOUT_SEC", "10.0")),
            1.0,
        )
        self._ws_open_timeout_sec = max(
            float(os.getenv("WS_OPEN_TIMEOUT_SEC", "15.0")),
            1.0,
        )
        self._messages_since_flush = 0

        stream_label = "+".join(s.value if isinstance(s, BinanceStreamType) else str(s) for s in self.streams)
        # 대시보드/Prometheus 라벨. 같은 class+symbol collector가 여러 개 떠도 구분한다.
        self._dash_name = f"{self.__class__.__name__}:{self.symbol.upper()}:{stream_label}"
        self._dash_idx = _dash_register(self._dash_name)
        self._symbol_label = self.symbol.upper()
        self._source_label = "websocket"
        self._consecutive_errors = 0

    @abstractmethod
    async def process_data(self, stream_name: str, payload: dict):
        """하위 클래스에서 데이터 정제 및 카프카 전송 로직 구현"""
        pass

    async def _send_to_kafka(self, stream_name: str, payload: dict):
        """Kafka로 메시지 전송 (고빈도 토픽은 deterministic subsharding 적용).

        run_in_executor로 Kafka send를 스레드풀에 오프로드하되,
        await 없이 fire-and-forget으로 WebSocket recv 루프 블로킹 방지.
        """
        try:
            topic: str = Config.get_topic(stream_name, self.market_type)
            symbol_upper: str = self.symbol.upper()
            ts_ms: int = int(time.time() * 1000)
            message = {
                "schema_version": DEFAULT_SCHEMA_VERSION,
                "event_time_ms": infer_event_time_ms(payload, topic),
                "ingested_at_ms": ts_ms,
                "source": stream_name,
                "collector": self.__class__.__name__,
                "symbol": symbol_upper,
                "topic": topic,
                "stream": stream_name,
                "data": payload,
                "ts": ts_ms,
            }
            validation = validate_collector_event(
                topic=topic,
                stream_name=stream_name,
                symbol=symbol_upper,
                payload=payload,
                ts_ms=ts_ms,
            )
            if not validation.ok:
                await self._send_bad_row(
                    topic=topic,
                    key=symbol_upper,
                    stream_name=stream_name,
                    message=message,
                    error_type=validation.error_type or "schema_validation_failed",
                    reason=validation.message,
                )
                return
            if not is_json_serializable(message):
                await self._send_bad_row(
                    topic=topic,
                    key=symbol_upper,
                    stream_name=stream_name,
                    message=message,
                    error_type="json_serialization_error",
                    reason="message is not JSON serializable",
                )
                return
            route = Config.build_partition_route(topic, symbol_upper, ts_ms, payload)
            self._record_source_success(stream_name, topic)
            # Pre-extract route values to avoid lambda closure capturing mutable object
            _route_key = route.key
            _route_partition = route.partition
            loop = asyncio.get_running_loop()
            loop.run_in_executor(
                None,
                lambda: self.kafka.send(
                    topic=topic,
                    value=message,
                    key=_route_key,
                    partition=_route_partition,
                    collector_name=self._dash_name,
                    symbol=symbol_upper,
                    source=stream_name,
                ),
            )
            kafka_messages_produced_total.labels(
                collector=self._dash_name,
                topic=topic,
            ).inc()
            if route.partition is not None:
                kafka_partition_key_bucket_total.labels(
                    topic=topic,
                    bucket_mod=str(route.partition),
                ).inc()
            self._messages_since_flush += 1
            if self.total_count <= self._startup_verify_messages:
                await loop.run_in_executor(None, self.kafka.flush)
        except Exception as e:
            collector_errors_total.labels(
                collector=self._dash_name,
                error_type="kafka_send",
            ).inc()
            self._record_error("kafka_send")
            _dash_log(f"❌ Kafka 전송 에러: {e}")

    async def _send_bad_row(
        self,
        *,
        topic: str,
        key: str,
        stream_name: str,
        message: dict,
        error_type: str,
        reason: str,
    ) -> None:
        collector_errors_total.labels(collector=self._dash_name, error_type=error_type).inc()
        self._record_error(error_type)
        _dash_log(f"⚠️ bad row 격리 | collector={self._dash_name} topic={topic} error_type={error_type}")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self.kafka.send_bad_row(
                original_topic=topic,
                payload=message,
                error_type=error_type,
                reason=reason,
                key=key,
                collector_name=self._dash_name,
                symbol=self._symbol_label,
                source=stream_name,
            ),
        )

    async def start(self):
        self.start_time = time.time()
        self.last_report_time = self.start_time
        record_process_heartbeat(
            collector=self._dash_name,
            symbol=self._symbol_label,
            source=self._source_label,
            topic="unknown",
            running=True,
        )
        _dash_log(f"🚀 {self._dash_name} 시작 | market: {self.market_type}")

        reconnect_delay = 3.0  # 첫 재연결 대기(초)
        max_delay = 60.0  # 최대 재연결 대기(초)

        while self.running:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=self._ws_ping_interval_sec,
                    ping_timeout=self._ws_ping_timeout_sec,
                    open_timeout=self._ws_open_timeout_sec,
                    close_timeout=5,
                    max_queue=4096,
                ) as ws:
                    connected_at = time.monotonic()
                    last_message_at = connected_at
                    saw_message = False
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=self._ws_recv_timeout_sec)
                            if not saw_message:
                                reconnect_delay = 3.0
                                saw_message = True
                            last_message_at = time.monotonic()
                            self.total_count += 1
                            self.sec_count += 1

                            try:
                                data = json.loads(msg)
                                stream_name = data.get("stream", "unknown")
                                payload = data.get("data", {})

                                if self.printed_samples < 5:
                                    _dash_log(f"📥 [{datetime.now().strftime('%H:%M:%S')}] {stream_name} 샘플 확인")
                                    self.printed_samples += 1

                                await self.process_data(stream_name, payload)

                            except json.JSONDecodeError as e:
                                _dash_log(f"❌ JSON 파싱 에러: {e}")
                                collector_errors_total.labels(collector=self._dash_name, error_type="json_parse").inc()
                                self._record_error("json_parse")
                            except Exception as e:
                                _dash_log(f"❌ 데이터 처리 에러: {e}")
                                collector_errors_total.labels(collector=self._dash_name, error_type="process").inc()
                                self._record_error("process")

                            await self._report_metrics()

                        except asyncio.TimeoutError:
                            idle_for = time.monotonic() - last_message_at
                            if idle_for >= self._ws_idle_reconnect_sec:
                                _dash_log(f"⚠️ {self._dash_name} 유휴 연결 감지 " f"({idle_for:.1f}s 무수신) — 재연결")
                                collector_errors_total.labels(
                                    collector=self._dash_name,
                                    error_type="ws_idle_timeout",
                                ).inc()
                                self._record_error("ws_idle_timeout")
                                websocket_reconnects_total.labels(collector=self._dash_name).inc()
                                self._record_reconnect("idle_timeout")
                                break
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            _dash_log(f"⚠️ {self._dash_name} 웹소켓 연결 끊어짐 — {reconnect_delay}초 후 재연결")
                            websocket_reconnects_total.labels(collector=self._dash_name).inc()
                            self._record_reconnect("connection_closed")
                            break  # inner loop 탈출 → outer loop에서 재연결

            except KeyboardInterrupt:
                self.running = False
                break
            except asyncio.CancelledError:
                self.running = False
                break
            except Exception as e:
                _dash_log(f"❌ {self._dash_name} 연결 에러: {e} — {reconnect_delay}초 후 재시도")
                websocket_reconnects_total.labels(collector=self._dash_name).inc()
                self._record_error("ws_connect_error")
                self._record_reconnect("connect_error")

            if not self.running:
                break

            # 지수 백오프로 재연결 대기
            sleep_for = min(reconnect_delay, max_delay)
            jittered_sleep = min(max_delay, random.uniform(sleep_for * 0.5, sleep_for * 1.5))
            await asyncio.sleep(jittered_sleep)
            reconnect_delay = min(reconnect_delay * 2, max_delay)
            _dash_log(
                f"🔄 {self._dash_name} 재연결 시도 중... "
                f"(backoff={reconnect_delay:.1f}s, sleep={jittered_sleep:.1f}s)"
            )

        self.kafka.close()
        record_process_heartbeat(
            collector=self._dash_name,
            symbol=self._symbol_label,
            source=self._source_label,
            topic="unknown",
            running=False,
        )
        self._final_report()

    async def _report_metrics(self):
        now = time.time()
        if now - self.last_report_time >= 1.0:
            tps = self.sec_count / (now - self.last_report_time)
            name = self._dash_name
            collector_tps.labels(collector=name).set(tps)
            collector_last_message_timestamp.labels(collector=name).set(now)
            record_process_heartbeat(
                collector=self._dash_name,
                symbol=self._symbol_label,
                source=self._source_label,
                topic="unknown",
                running=self.running,
                timestamp=now,
            )
            _dash_update(
                self._dash_idx,
                f"[{name}] TPS: {tps:.2f} msgs/sec | 누적: {self.total_count:,}",
            )
            self.sec_count = 0
            self.last_report_time = now

    def _record_source_success(self, stream_name: str, topic: str) -> None:
        self._consecutive_errors = 0
        source = stream_name or self._source_label
        record_source_success(
            collector=self._dash_name,
            symbol=self._symbol_label,
            source=source,
            topic=topic,
        )
        reset_collector_error(
            collector=self._dash_name,
            symbol=self._symbol_label,
            source=source,
            error_type="websocket",
        )

    def _record_error(self, error_type: str) -> None:
        self._consecutive_errors += 1
        record_collector_error(
            collector=self._dash_name,
            symbol=self._symbol_label,
            source=self._source_label,
            error_type=error_type,
            consecutive_errors=self._consecutive_errors,
        )

    def _record_reconnect(self, reason: str) -> None:
        record_collector_reconnect(
            collector=self._dash_name,
            symbol=self._symbol_label,
            source=self._source_label,
            reason=reason,
        )

    def _final_report(self):
        if self.start_time:
            duration = time.time() - self.start_time
            avg_tps = self.total_count / duration if duration > 0 else 0
            _dash_log(f"📊 종료 | {self._dash_name} | 평균 TPS: {avg_tps:.2f} | 총 메시지: {self.total_count:,}")
