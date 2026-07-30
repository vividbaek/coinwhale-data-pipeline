# Contributing

Keep changes focused on one pipeline boundary: collectors, Kafka contracts,
Spark, ClickHouse, dbt, orchestration, or observability.

Before opening a pull request:

```bash
python -m pip install -r requirements-dev.txt
make test
make compile
make compose-check
```

For dbt changes, also run `make dbt-parse`.

Do not commit runtime data, credentials, private infrastructure details, or
performance claims without a reproducible command and dated environment.
