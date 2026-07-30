from __future__ import annotations

import unittest

from common.collector_schema import validate_collector_event
from common.topic_contracts import contract_topics, get_topic_contract


class CollectorSchemaTests(unittest.TestCase):
    def test_valid_trade_payload_passes(self) -> None:
        result = validate_collector_event(
            topic="binance-trade",
            stream_name="aggTrade",
            symbol="BTCUSDT",
            payload={"p": "77830.4", "q": "0.01", "T": 1710000000000, "a": 123},
            ts_ms=1710000000000,
        )

        self.assertTrue(result.ok)

    def test_invalid_timestamp_fails_when_present(self) -> None:
        result = validate_collector_event(
            topic="binance-trade",
            stream_name="aggTrade",
            symbol="BTCUSDT",
            payload={"p": "77830.4", "q": "0.01", "T": "not-a-time"},
            ts_ms=1710000000000,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.error_type, "schema_timestamp_invalid")

    def test_invalid_numeric_field_fails(self) -> None:
        result = validate_collector_event(
            topic="binance-bookticker",
            stream_name="bookTicker",
            symbol="ETHUSDT",
            payload={"b": "not-a-price", "a": "2172.91"},
            ts_ms=1710000000000,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.error_type, "schema_bookticker_bid_invalid")

    def test_funding_rate_in_mark_price_must_be_numeric_when_present(self) -> None:
        result = validate_collector_event(
            topic="binance-markprice",
            stream_name="markPrice",
            symbol="SOLUSDT",
            payload={"p": "85.92", "r": "bad-rate", "T": 1710000000000},
            ts_ms=1710000000000,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.error_type, "schema_funding_rate_invalid")


    def test_hot_path_contracts_are_registered(self) -> None:
        self.assertTrue(
            {
                "binance-trade",
                "spot-trade",
                "binance-bookticker",
                "spot-bookticker",
                "binance-markprice",
                "binance-openinterest",
            }.issubset(contract_topics())
        )
        self.assertEqual(get_topic_contract("spot-trade")["required_fields"]["p"], "number")

    def test_future_event_time_fails_before_kafka(self) -> None:
        result = validate_collector_event(
            topic="binance-trade",
            stream_name="aggTrade",
            symbol="BTCUSDT",
            payload={"p": "77830.4", "q": "0.01", "T": 1710000120001},
            ts_ms=1710000000000,
        )

        self.assertEqual(result.error_type, "schema_event_time_future")

    def test_crossed_bookticker_fails_before_aggregation(self) -> None:
        result = validate_collector_event(
            topic="binance-bookticker",
            stream_name="bookTicker",
            symbol="ETHUSDT",
            payload={"b": "2173.00", "a": "2172.91"},
            ts_ms=1710000000000,
        )

        self.assertEqual(result.error_type, "schema_bid_ask_crossed")

    def test_open_interest_alias_is_validated_from_contract(self) -> None:
        ok = validate_collector_event(
            topic="binance-openinterest",
            stream_name="openInterest",
            symbol="BTCUSDT",
            payload={"open_interest": "12345.6"},
            ts_ms=1710000000000,
        )
        bad = validate_collector_event(
            topic="binance-openinterest",
            stream_name="openInterest",
            symbol="BTCUSDT",
            payload={"openInterest": "not-a-number"},
            ts_ms=1710000000000,
        )

        self.assertTrue(ok.ok)
        self.assertEqual(bad.error_type, "schema_openinterest_invalid")


if __name__ == "__main__":
    unittest.main()
