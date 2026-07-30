from __future__ import annotations

import unittest
from unittest.mock import patch

from collectors import base_collector
from collectors import rest_poller
from collectors.run_all import create_collectors_for_symbol
from common.config import Config
from utils.binance_stream_enum import BinanceStreamType


class FakeKafka:
    def __init__(self, *_args, **_kwargs) -> None:
        self.sent: list[dict] = []
        self.bad_rows: list[dict] = []
        self.flush_count = 0

    def send(self, **kwargs) -> None:
        self.sent.append(kwargs)

    def send_bad_row(self, **kwargs) -> None:
        self.bad_rows.append(kwargs)

    def flush(self, *_args, **_kwargs) -> None:
        self.flush_count += 1

    def close(self, *_args, **_kwargs) -> None:
        pass


class DummyCollector(base_collector.BaseBinanceCollector):
    async def process_data(self, stream_name: str, payload: dict):
        await self._send_to_kafka(stream_name, payload)


class BaseCollectorHealthTests(unittest.IsolatedAsyncioTestCase):
    def test_default_futures_websocket_url_uses_official_host(self) -> None:
        self.assertEqual(
            Config.FUTURES_PUBLIC_WS_URL,
            "wss://fstream.binance.com/public/stream?streams=",
        )
        self.assertEqual(
            Config.FUTURES_MARKET_WS_URL,
            "wss://fstream.binance.com/market/stream?streams=",
        )

    def test_futures_collector_selects_public_or_market_endpoint(self) -> None:
        with patch.object(base_collector, "KafkaProducerWrapper", return_value=FakeKafka()):
            with patch.object(base_collector, "_dash_register", return_value=0):
                public_collector = DummyCollector("btcusdt", [BinanceStreamType.BOOK_TICKER])
                market_collector = DummyCollector("btcusdt", [BinanceStreamType.AGG_TRADE])

        self.assertTrue(public_collector.url.startswith(Config.FUTURES_PUBLIC_WS_URL))
        self.assertTrue(market_collector.url.startswith(Config.FUTURES_MARKET_WS_URL))

    def test_mixed_futures_streams_are_rejected(self) -> None:
        with patch.object(base_collector, "KafkaProducerWrapper", return_value=FakeKafka()):
            with patch.object(base_collector, "_dash_register", return_value=0):
                with self.assertRaises(ValueError):
                    DummyCollector(
                        "btcusdt",
                        [BinanceStreamType.DEPTH, BinanceStreamType.AGG_TRADE],
                    )

    def test_run_all_does_not_mix_futures_public_and_market_streams(self) -> None:
        with patch.object(base_collector, "KafkaProducerWrapper", return_value=FakeKafka()):
            with patch.object(rest_poller, "KafkaProducerWrapper", return_value=FakeKafka()):
                with patch.object(base_collector, "_dash_register", return_value=0):
                    ws_collectors, _rest_pollers = create_collectors_for_symbol("DOGEUSDT")

        futures_urls = [
            collector.url
            for collector in ws_collectors
            if collector.market_type == "futures"
        ]

        self.assertIn(
            f"{Config.FUTURES_PUBLIC_WS_URL}dogeusdt@depth@100ms",
            futures_urls,
        )
        self.assertIn(
            f"{Config.FUTURES_PUBLIC_WS_URL}dogeusdt@bookTicker",
            futures_urls,
        )
        self.assertIn(
            f"{Config.FUTURES_MARKET_WS_URL}dogeusdt@kline_1m/dogeusdt@aggTrade",
            futures_urls,
        )
        self.assertIn(
            f"{Config.FUTURES_MARKET_WS_URL}dogeusdt@miniTicker/dogeusdt@ticker",
            futures_urls,
        )
        self.assertTrue(
            all(
                not (
                    collector.url.startswith(Config.FUTURES_PUBLIC_WS_URL)
                    and any(
                        token in collector.url
                        for token in (
                            "aggTrade",
                            "kline",
                            "markPrice",
                            "forceOrder",
                            "miniTicker",
                            "ticker",
                        )
                    )
                )
                for collector in ws_collectors
                if collector.market_type == "futures"
            )
        )
        self.assertTrue(
            all(
                not (
                    collector.url.startswith(Config.FUTURES_MARKET_WS_URL)
                    and any(token in collector.url for token in ("bookTicker", "depth"))
                )
                for collector in ws_collectors
                if collector.market_type == "futures"
            )
        )

    async def test_send_to_kafka_passes_symbol_and_source_for_delivery_health(
        self,
    ) -> None:
        fake_kafka = FakeKafka()
        with patch.object(
            base_collector, "KafkaProducerWrapper", return_value=fake_kafka
        ):
            with patch.object(base_collector, "_dash_register", return_value=0):
                collector = DummyCollector("btcusdt", ["aggTrade"])

        await collector._send_to_kafka(
            "aggTrade", {"p": "1.0", "q": "0.2", "T": 1710000000000}
        )

        self.assertEqual(fake_kafka.sent[0]["topic"], "binance-trade")
        self.assertEqual(fake_kafka.sent[0]["symbol"], "BTCUSDT")
        self.assertEqual(fake_kafka.sent[0]["source"], "aggTrade")
        self.assertEqual(fake_kafka.sent[0]["collector_name"], collector._dash_name)

    async def test_invalid_payload_goes_to_bad_row_without_normal_send(self) -> None:
        fake_kafka = FakeKafka()
        with patch.object(
            base_collector, "KafkaProducerWrapper", return_value=fake_kafka
        ):
            with patch.object(base_collector, "_dash_register", return_value=0):
                collector = DummyCollector("btcusdt", ["aggTrade"])

        await collector._send_to_kafka("aggTrade", {"p": "1.0", "T": 1710000000000})

        self.assertEqual(fake_kafka.sent, [])
        self.assertEqual(len(fake_kafka.bad_rows), 1)
        self.assertEqual(fake_kafka.bad_rows[0]["original_topic"], "binance-trade")
        self.assertEqual(
            fake_kafka.bad_rows[0]["error_type"], "schema_trade_quantity_invalid"
        )
        self.assertEqual(fake_kafka.bad_rows[0]["key"], "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
