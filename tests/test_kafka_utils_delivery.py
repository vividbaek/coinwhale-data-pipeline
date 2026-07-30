from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from common.kafka_utils import KafkaProducerWrapper


class FakeFuture:
    def __init__(self) -> None:
        self.callbacks: list[Any] = []
        self.errbacks: list[Any] = []

    def add_callback(self, callback):
        self.callbacks.append(callback)
        return self

    def add_errback(self, callback):
        self.errbacks.append(callback)
        return self

    def succeed(self) -> None:
        for callback in list(self.callbacks):
            callback(object())

    def fail(self, exception: Exception) -> None:
        for errback in list(self.errbacks):
            errback(exception)


class FakeProducer:
    def __init__(self, *, raise_on_send: bool = False) -> None:
        self.raise_on_send = raise_on_send
        self.sent: list[dict[str, Any]] = []
        self.futures: list[FakeFuture] = []
        self.flush_calls: list[Any] = []
        self.close_calls: list[Any] = []

    def send(self, topic, value, key=None, partition=None):
        if self.raise_on_send:
            raise RuntimeError("send failed password=super-secret")
        future = FakeFuture()
        self.sent.append(
            {"topic": topic, "value": value, "key": key, "partition": partition}
        )
        self.futures.append(future)
        return future

    def flush(self, timeout=None):
        self.flush_calls.append(timeout)

    def close(self, timeout=None):
        self.close_calls.append(timeout)


class FakeConfluentProducer:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.poll_calls: list[float] = []

    def produce(self, topic, value=None, key=None, partition=None, on_delivery=None):
        self.sent.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "partition": partition,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout):
        self.poll_calls.append(timeout)

    def flush(self, timeout=None):
        return 0


def _wrapper(producer: FakeProducer | None = None) -> KafkaProducerWrapper:
    wrapper = object.__new__(KafkaProducerWrapper)
    wrapper.dlq_topic = "dlq-producer"
    wrapper.bootstrap_servers = "unit-test:9092"
    wrapper._backend = "kafka-python"
    wrapper._init_runtime_state()
    wrapper.producer = producer or FakeProducer()
    return wrapper


def _confluent_wrapper(producer: FakeConfluentProducer | None = None) -> KafkaProducerWrapper:
    wrapper = object.__new__(KafkaProducerWrapper)
    wrapper.dlq_topic = "dlq-producer"
    wrapper.bootstrap_servers = "unit-test:9092"
    wrapper._backend = "confluent"
    wrapper._init_runtime_state()
    wrapper.producer = producer or FakeConfluentProducer()
    return wrapper


class KafkaProducerDeliveryTests(unittest.TestCase):
    def test_delivery_callback_success_updates_counters(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)

        with patch(
            "common.collector_health.record_kafka_send_success"
        ) as record_success:
            wrapper.send(
                "binance-trade",
                {"symbol": "BTCUSDT", "data": {"price": 1}},
                key="BTCUSDT",
                collector_name="collector-a",
                symbol="BTCUSDT",
                source="aggTrade",
            )
            producer.futures[0].succeed()

        self.assertEqual(wrapper.send_attempt_count, 1)
        self.assertEqual(wrapper.delivery_success_count, 1)
        self.assertEqual(wrapper.delivery_failure_count, 0)
        self.assertEqual(wrapper._pending_deliveries, 0)
        record_success.assert_called_once_with(
            collector="collector-a",
            symbol="BTCUSDT",
            source="aggTrade",
            topic="binance-trade",
        )

    def test_delivery_callback_failure_sends_one_dlq_message(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)

        wrapper.send(
            "binance-trade",
            {
                "symbol": "BTCUSDT",
                "stream": "trade",
                "data": {"price": 1, "token": "abc123"},
            },
            key="BTCUSDT",
        )
        producer.futures[0].fail(RuntimeError("broker timeout"))

        self.assertEqual(wrapper.delivery_failure_count, 1)
        self.assertEqual(wrapper._pending_deliveries, 0)
        self.assertEqual(len(producer.sent), 2)
        self.assertEqual(producer.sent[1]["topic"], "dlq-producer")
        self.assertEqual(producer.sent[1]["value"]["original_topic"], "binance-trade")
        self.assertEqual(producer.sent[1]["value"]["symbol"], "BTCUSDT")
        self.assertNotIn("abc123", producer.sent[1]["value"]["payload_preview"])

    def test_confluent_delivery_callback_failure_uses_local_fallback(self) -> None:
        producer = FakeConfluentProducer()
        wrapper = _confluent_wrapper(producer)

        with patch("common.kafka_utils.write_bad_row_fallback") as fallback:
            wrapper.send(
                "binance-trade",
                {
                    "symbol": "BTCUSDT",
                    "stream": "trade",
                    "data": {"price": 1},
                },
                key="BTCUSDT",
            )
            producer.sent[0]["on_delivery"](RuntimeError("not enough replicas"), None)

        self.assertEqual(wrapper.delivery_failure_count, 1)
        self.assertEqual(wrapper.dlq_failure_count, 1)
        self.assertEqual(wrapper._pending_deliveries, 0)
        self.assertEqual(len(producer.sent), 1)
        fallback.assert_called_once()

    def test_dlq_delivery_failure_does_not_recurse(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)

        wrapper.send(
            "binance-trade", {"symbol": "BTCUSDT", "data": {"price": 1}}, key="BTCUSDT"
        )
        producer.futures[0].fail(RuntimeError("broker timeout"))
        producer.futures[1].fail(RuntimeError("dlq unavailable"))

        self.assertEqual(len(producer.sent), 2)
        self.assertEqual(wrapper.dlq_failure_count, 1)

    def test_send_bad_row_uses_dlq_and_masks_payload_preview(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)

        wrapper.send_bad_row(
            original_topic="binance-trade",
            payload={"symbol": "BTCUSDT", "token": "abc123"},
            error_type="schema_payload_invalid",
            reason="payload must be an object",
            key="BTCUSDT",
            collector_name="collector-a",
            symbol="BTCUSDT",
            source="aggTrade",
        )

        self.assertEqual(len(producer.sent), 1)
        self.assertEqual(producer.sent[0]["topic"], "dlq-producer")
        self.assertEqual(producer.sent[0]["value"]["original_topic"], "binance-trade")
        self.assertNotIn("abc123", producer.sent[0]["value"]["payload_preview"])

    def test_send_bad_row_dlq_failure_writes_local_fallback(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)

        with patch("common.kafka_utils.write_bad_row_fallback") as fallback:
            wrapper.send_bad_row(
                original_topic="binance-trade",
                payload={"symbol": "BTCUSDT"},
                error_type="schema_payload_invalid",
                reason="bad",
                key="BTCUSDT",
            )
            producer.futures[0].fail(RuntimeError("dlq unavailable"))

        fallback.assert_called_once()
        self.assertEqual(wrapper.dlq_failure_count, 1)

    def test_send_exception_records_failure_and_masks_secret(self) -> None:
        producer = FakeProducer(raise_on_send=True)
        wrapper = _wrapper(producer)

        with self.assertLogs("common.kafka_utils", level="ERROR") as logs:
            wrapper.send(
                "binance-trade",
                {"symbol": "BTCUSDT", "data": {"token": "abc123"}},
                key="BTCUSDT",
            )

        output = "\n".join(logs.output)
        self.assertEqual(wrapper.delivery_failure_count, 1)
        self.assertNotIn("super-secret", output)
        self.assertIn("password=***", output)

    def test_close_flushes_once_and_is_idempotent(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)

        wrapper.close(timeout=2.0)
        wrapper.close(timeout=2.0)

        self.assertEqual(producer.flush_calls, [2.0])
        self.assertEqual(producer.close_calls, [2.0])

    def test_flush_with_pending_callbacks_records_timeout_warning(self) -> None:
        producer = FakeProducer()
        wrapper = _wrapper(producer)
        wrapper._pending_deliveries = 1

        with self.assertLogs("common.kafka_utils", level="WARNING"):
            wrapper.flush(timeout=0.1)

        self.assertEqual(wrapper.flush_timeout_count, 1)


if __name__ == "__main__":
    unittest.main()
