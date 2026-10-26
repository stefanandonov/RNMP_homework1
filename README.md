# RNMP homework 1 – Kafka setup

Requirements: Docker (with Compose v2) and either [uv](https://docs.astral.sh/uv/) (recommended) or Python 3.9+.

## Run

```sh
./start.sh
```

This starts Kafka on `localhost:9092`, installs the Python dependencies and begins
producing random sensor readings to the `sensors` topic.

In a second terminal, read messages back:

```sh
./run.sh consume_messages.py           # topic 'sensors'
./run.sh consume_messages.py result1   # any other topic, e.g. Flink output
```

`run.sh` uses `uv` when installed (dependencies from `pyproject.toml`/`uv.lock`, no manual venv).
Without `uv` it falls back to a local `venv` and `requirements.txt`. Install uv with `brew install uv`.

Stop and clean up with `./stop.sh`.

## Connecting from other containers (e.g. Flink)

Containers on the same compose network use `kafka:9093`; programs on your machine use `localhost:9092`.

Works natively on Intel and Apple Silicon (M1/M2/M3) – no emulation.
