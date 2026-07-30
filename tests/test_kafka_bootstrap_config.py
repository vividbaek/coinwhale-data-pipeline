from common.config import Config, normalize_kafka_bootstrap_servers


def test_default_local_broker_is_explicit() -> None:
    assert normalize_kafka_bootstrap_servers(None) == "localhost:9092"


def test_explicit_broker_list_is_preserved_and_deduplicated() -> None:
    assert (
        normalize_kafka_bootstrap_servers("kafka-a:9092,kafka-b:9092,kafka-a:9092")
        == "kafka-a:9092,kafka-b:9092"
    )


def test_demo_config_uses_local_broker() -> None:
    assert Config.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"
