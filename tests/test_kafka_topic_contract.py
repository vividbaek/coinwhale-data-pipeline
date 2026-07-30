from common.config import Config
from common.topic_contracts import get_topic_contract


def test_collector_streams_map_to_named_topics() -> None:
    expected = {
        "aggTrade": "binance-trade",
        "depth": "binance-depth",
        "bookTicker": "binance-bookticker",
        "forceOrder": "binance-liquidation",
        "markPrice": "binance-markprice",
        "openInterest": "binance-openinterest",
    }
    for stream, topic in expected.items():
        assert Config.get_topic(stream) == topic


def test_spot_streams_use_spot_topics() -> None:
    assert Config.get_topic("aggTrade", market_type="spot") == "spot-trade"
    assert Config.get_topic("bookTicker", market_type="spot") == "spot-bookticker"


def test_topic_contracts_define_collector_payloads() -> None:
    for topic in (
        "binance-trade",
        "binance-bookticker",
        "binance-markprice",
        "binance-openinterest",
        "spot-trade",
    ):
        contract = get_topic_contract(topic)
        assert contract is not None
        assert contract.get("required_fields") or contract.get("required_any")


def test_hot_topics_use_more_partitions_than_regular_topics() -> None:
    assert Config.get_topic_partition_count("binance-trade") > Config.get_topic_partition_count(
        "binance-funding"
    )
