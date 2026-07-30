import importlib
import math
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPARK_JOBS = PROJECT_ROOT / "spark_jobs"


def _load_policy():
    sys.path.insert(0, str(SPARK_JOBS))
    sys.modules.pop("row_hash_policy", None)
    return importlib.import_module("row_hash_policy")


def _price_row(**overrides):
    row = {
        "symbol": "BTCUSDT",
        "ts": "2026-05-17 00:00:00.123456",
        "futures_bid": 100.1234567890123,
        "futures_ask": 101.0,
        "futures_spread": 0.8765432109876,
        "spot_bid": 99.5,
        "spot_ask": 100.5,
        "spot_spread": 1.0,
        "basis_pct": Decimal("0.12345678901234"),
        "mark_price": 100.25,
        "funding_rate": Decimal("0.0001000000004"),
        "index_price": 100.2,
    }
    row.update(overrides)
    return row


class RowHashPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = _load_policy()

    def test_same_row_has_same_hash_regardless_of_dict_order(self):
        first = _price_row()
        second = dict(reversed(list(first.items())))

        self.assertEqual(
            self.policy.build_row_hash("stream_shadow.price", first),
            self.policy.build_row_hash("stream_shadow.price", second),
        )

    def test_one_value_change_changes_hash(self):
        original = self.policy.build_row_hash(
            "stream_shadow.price", _price_row(mark_price=100.25)
        )
        changed = self.policy.build_row_hash(
            "stream_shadow.price", _price_row(mark_price=100.26)
        )

        self.assertNotEqual(original, changed)

    def test_null_and_nan_normalize_to_same_sentinel(self):
        self.assertEqual(self.policy.normalize_hash_value(None), "__NULL__")
        self.assertEqual(self.policy.normalize_hash_value(math.nan), "__NULL__")
        self.assertEqual(self.policy.normalize_hash_value(Decimal("NaN")), "__NULL__")

    def test_timestamp_timezone_normalization_is_deterministic(self):
        aware = datetime(2026, 5, 17, 9, 0, 0, 123999, tzinfo=timezone.utc)
        text = "2026-05-17T09:00:00.123999+00:00"

        self.assertEqual(
            self.policy.normalize_hash_value(aware, value_type="timestamp"),
            "2026-05-17T09:00:00.123Z",
        )
        self.assertEqual(
            self.policy.normalize_hash_value(aware, value_type="timestamp"),
            self.policy.normalize_hash_value(text, value_type="timestamp"),
        )

    def test_float_precision_normalization_is_deterministic(self):
        self.assertEqual(
            self.policy.normalize_hash_value(1.2300000000004, value_type="number"),
            "1.23",
        )
        self.assertEqual(
            self.policy.normalize_hash_value(
                Decimal("1.2300000000004"), value_type="number"
            ),
            "1.23",
        )

    def test_unknown_table_fails_clearly(self):
        with self.assertRaisesRegex(
            self.policy.RowHashPolicyError, "unknown row_hash table"
        ):
            self.policy.build_row_hash("stream_shadow.unknown", {})

    def test_optional_column_missing_is_allowed(self):
        row = {
            "symbol": "BTCUSDT",
            "ts": "2026-05-17T00:00:00Z",
            "price_change_pct": 1.0,
            "weighted_avg_price": 100.0,
            "last_price": 101.0,
            "volume_24h": 10.0,
            "quote_volume_24h": 1000.0,
            "high_24h": 110.0,
            "low_24h": 90.0,
            "open_price_24h": 95.0,
        }

        row_hash = self.policy.build_row_hash("stream_shadow.market_metrics", row)

        self.assertEqual(len(row_hash), 64)

    def test_missing_required_column_fails_clearly(self):
        row = _price_row()
        row.pop("mark_price")

        with self.assertRaisesRegex(
            self.policy.RowHashPolicyError, "missing required row_hash columns"
        ):
            self.policy.build_row_hash("stream_shadow.price", row)

    def test_secret_like_non_hash_field_is_not_included(self):
        secret = "never-log-this-secret"
        row_with_secret = _price_row(password=secret, api_key=secret)
        row_without_secret = _price_row()

        self.assertEqual(
            self.policy.build_row_hash("stream_shadow.price", row_with_secret),
            self.policy.build_row_hash("stream_shadow.price", row_without_secret),
        )

    def test_stream_table_alias_uses_shadow_policy(self):
        self.assertEqual(
            self.policy.build_row_hash("stream.price", _price_row()),
            self.policy.build_row_hash("stream_shadow.price", _price_row()),
        )


if __name__ == "__main__":
    unittest.main()
