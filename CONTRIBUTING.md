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

For dbt changes, also run `make dbt-parse` and validate `make dbt-build`
against an isolated ClickHouse container. For Spark quality changes, install
`requirements-spark-test.txt` and run `make test-spark`.

Do not commit runtime data, credentials, private infrastructure details, or
performance claims without a reproducible command and dated environment.

Review [docs/PUBLIC_SCOPE.md](docs/PUBLIC_SCOPE.md) before adding a new source
family. This public repository is an allowlisted reference project, not a
mirror of the private CoinWhale workspace.
