# Public Scope and Release Gate

This repository is an allowlisted extraction for data-engineering learning and
portfolio review. It is not a mirror of the private CoinWhale workspace.

## Included

- public-market WebSocket and REST collectors;
- generic Kafka envelope, partition-routing, delivery, and DLQ code;
- Spark parsing, event-time windows, watermarks, checkpoints, data-quality
  gates, and ClickHouse `foreachBatch` writers;
- local-only ClickHouse DDL and Docker Compose configuration;
- dbt source, staging, mart, freshness, and data-test examples;
- one minimal Airflow dbt DAG;
- local Prometheus scrape configuration;
- dependency-light tests, optional Spark tests, integration CI, and public
  runbooks.

## Excluded

- `.env` files, credentials, tokens, webhooks, private keys, and credential
  history;
- ClickHouse/Kafka volumes, raw events, parquet exports, backups, checkpoints,
  DLQ payloads, quarantine payloads, logs, and dbt artifacts;
- private hostnames, IP addresses, ports, user grants, network topology, cloud
  tunnels, and deployment credentials;
- trading execution, paper/shadow trading, strategies, signals, backtests,
  model artifacts, and research datasets;
- agents, RAG, LLM prompts, provider configuration, and internal evaluation
  reports;
- incident reports, internal dashboards, alert inventories, operational
  evidence, and unpublished scale or performance measurements;
- user or customer data of any kind.

## Publication claims

Public copy may describe implemented architecture and reproducible local test
results. It must not claim a particular production throughput, uptime, data
volume, freshness, trading outcome, cost saving, or user impact without a
separate dated and reviewed evidence artifact.

## Release gate

Before every public push:

1. Review `git diff --cached` and the complete tracked-file list.
2. Confirm `.env`, runtime data, logs, checkpoints, dbt artifacts, and local
   virtual environments remain ignored.
3. Run contract tests, compilation, Compose validation, dbt parse/build, and
   the optional Spark quality suite.
4. Scan both the working tree and complete Git history with Gitleaks.
5. Search for private paths, identities, hostnames, credentials, and internal
   project names.
6. Run the workspace Git identity guard for the target repository.
7. Confirm README claims still match code and same-revision command output.

Runtime and test dependencies are exact pins. Dependabot updates should change
those pins deliberately; do not merge a minimum-version-only range rewrite.

## License boundary

The repository is publicly visible, but no reuse license has been selected.
Adding a license is a maintainer decision and should not be inferred from
visibility alone.
