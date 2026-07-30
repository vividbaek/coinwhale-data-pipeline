# spark_jobs/silver_aggregator.py
"""
Silver Layer Aggregator — Kafka 원본 데이터를 event-time window로 집계해 ClickHouse Silver 테이블에 적재.

새 Silver 테이블 추가 방법:
  1. spark_jobs/silver/ 에 파일 생성 (예: price.py)
  2. SilverBase 상속 → TOPIC / TABLE / CHECKPOINT / ENABLED 선언
  3. parse() / aggregate() / select_output() 구현
  4. 이 파일(silver_aggregator.py)은 수정 불필요 — 자동 탐색됨

비활성화:
  각 Silver 클래스의 ENABLED = False 로 설정하면 aggregator가 스킵

실행:
  ./scripts/start-silver-aggregator.sh
"""

from __future__ import annotations

import importlib
import inspect
import os
import pkgutil
import signal
import socket
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from silver.base import SilverBase as _SilverBase

import silver
from kafka_reader import create_spark_session
from silver.base import SilverBase


def parse_job_filters() -> set[str]:
    """환경변수로 전달된 실행 대상 table/class 이름 집합."""
    raw = os.getenv("SILVER_JOB_TABLES", "").strip()
    if not raw:
        return set()
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def wait_for_kafka(
    host: str | None = None,
    port: int | None = None,
    timeout: int = 60,
) -> None:
    """Kafka 포트가 열릴 때까지 소켓으로 대기 (최대 timeout초)."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092").split(",", 1)[0]
    default_host, _, default_port = bootstrap.partition(":")
    host = host or default_host
    port = port or int(default_port or "29092")
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("Kafka 연결 확인 완료")
                return
        except OSError:
            print("Kafka 대기 중...", end="\r", flush=True)
            time.sleep(1)
    raise RuntimeError(f"Kafka({host}:{port}) 연결 실패 (timeout={timeout}s)")


def discover_silver_jobs(job_filters: set[str] | None = None) -> list[SilverBase]:
    """
    silver/ 패키지 내 모든 모듈을 순회하여
    SilverBase 서브클래스를 자동 탐색 후 인스턴스 반환.

    탐색 대상: silver/*.py (base.py 제외)
    조건: SilverBase 서브클래스 + ENABLED = True
    """
    found: list[SilverBase] = []
    include_raw = os.getenv("SILVER_INCLUDE", "").strip()
    exclude_raw = os.getenv("SILVER_EXCLUDE", "").strip()
    include_set = {x.strip() for x in include_raw.split(",") if x.strip()}
    exclude_set = {x.strip() for x in exclude_raw.split(",") if x.strip()}

    for _, module_name, _ in pkgutil.iter_modules(silver.__path__):
        if module_name == "base":
            continue
        module = importlib.import_module(f"silver.{module_name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, SilverBase) and obj is not SilverBase and obj.__module__ == module.__name__:
                instance = obj()
                class_name = obj.__name__
                table_name = getattr(instance, "TABLE", "")
                aliases = {class_name, table_name}

                if include_set and aliases.isdisjoint(include_set):
                    continue
                if exclude_set and not aliases.isdisjoint(exclude_set):
                    continue

                found.append(instance)

    found.sort(key=lambda job: job.TABLE)
    if not job_filters:
        return found

    return [job for job in found if job.TABLE.lower() in job_filters or job.__class__.__name__.lower() in job_filters]


def main() -> None:
    app_name = os.getenv("SILVER_APP_NAME", "SilverAggregator")
    job_filters = parse_job_filters()

    spark = create_spark_session(app_name)
    wait_for_kafka()

    jobs = discover_silver_jobs(job_filters)
    if job_filters and not jobs:
        raise RuntimeError(f"SILVER_JOB_TABLES={sorted(job_filters)} 에 해당하는 Silver Job을 찾지 못했습니다.")

    filter_label = ",".join(sorted(job_filters)) if job_filters else "ALL"
    print(f"\nSpark app: {app_name}")
    print(f"실행 대상 필터: {filter_label}")
    print(f"\n탐색된 Silver Job: {len(jobs)}개")
    for job in jobs:
        status = "✅" if job.ENABLED else "⬜ (ENABLED=False)"
        print(f"  {status} {job.__class__.__name__:30s} → topic={job.TOPIC}, table={job.TABLE}")
    print()

    queries = []
    for job in jobs:
        q = job.start(spark)
        if q is not None:
            queries.append(q)

    print(f"\n실행 중인 쿼리: {len(queries)}개")

    def _stop_all_streams() -> None:
        for q in list(spark.streams.active):
            try:
                q.stop()
            except Exception as e:
                print(f"스트리밍 쿼리 stop 실패: {e}")
        try:
            spark.stop()
        except Exception as e:
            print(f"Spark stop 실패: {e}")

    def _graceful_shutdown(signum: int, frame: object) -> None:
        print("\nSIGTERM 수신 — 스트리밍 쿼리 graceful stop 중...")
        _stop_all_streams()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        print("스트리밍 쿼리 비정상 종료 감지 — 남은 쿼리 정리 후 프로세스 종료")
        raise
    finally:
        _stop_all_streams()


if __name__ == "__main__":
    main()
