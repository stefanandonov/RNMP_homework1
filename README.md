# RNMP homework 1 – Kafka setup

This repo gives you a local Apache Kafka broker and two small Python programs:

- a **producer** that sends random sensor readings to the Kafka topic `sensors`
- a **consumer** that reads messages from a topic and prints them

You will use this as the data source for the homework (e.g. a Flink job that reads `sensors`
and writes its results to other topics such as `result1` and `result2`). The consumer lets you
check what is in any topic.

## What you need

1. **Docker Desktop** (or Docker Engine), running. Check with `docker compose version`.
2. **uv** (recommended), a fast Python package manager: `brew install uv`
   or see https://docs.astral.sh/uv/getting-started/installation/.
   If you don't want uv, Python 3.9+ is enough, and the scripts fall back to a plain `venv`.

Works on Windows (use Git Bash or WSL to run the `.sh` scripts), Linux, and Intel and
Apple Silicon (M1/M2/M3) Macs.

## Quick start

Open **two terminals** in this folder.

**Terminal 1** – start Kafka and begin producing data:

```sh
./start.sh
```

The first run downloads the Kafka image and may take a minute or two. When it works you will
see a new JSON record every 0.5–2 seconds:

```
{"key": "C", "value": 277, "timestamp": 1793056486423}
```

Leave it running.

**Terminal 2** – read the messages back:

```sh
./run.sh consume_messages.py            # reads topic 'sensors'
./run.sh consume_messages.py result1    # reads any other topic, e.g. your Flink output
```

Stop either program with `Ctrl+C`. When you are done for the day, stop Kafka:

```sh
./stop.sh
```

`./stop.sh` also deletes all data stored in Kafka, so you start clean next time.

## What each file does

| File | Purpose |
|---|---|
| `docker-compose.yml` | Defines the Kafka broker container. |
| `start.sh` | Starts Kafka, waits until it is ready, then runs the producer. |
| `stop.sh` | Stops Kafka and deletes its data. |
| `run.sh` | Runs a Python script with its dependencies installed (uses uv if available, otherwise a `venv`). |
| `produce_messages.py` | Sends a random record to topic `sensors` every 0.5–2 s. |
| `consume_messages.py` | Prints every message of a topic. Topic is the first argument (default `sensors`). |
| `pyproject.toml`, `uv.lock` | Python dependencies for uv. |
| `requirements.txt` | The same dependencies for plain `pip` (fallback without uv). |

## The data

Each message in `sensors` is a JSON object:

| Field | Meaning |
|---|---|
| `key` | The sensor: one of `A`, `B`, `C`, `D` |
| `value` | A random integer from 0 to 1000 |
| `timestamp` | Creation time in milliseconds since the Unix epoch (event time) |

## How the pieces connect

```
produce_messages.py ──► Kafka (topic 'sensors') ──► your Flink job ──► Kafka (topics 'result1', 'result2')
                                   │                                              │
                                   └──────────► consume_messages.py ◄─────────────┘
                                                (to inspect any topic)
```

Kafka is reachable at two addresses, depending on where your program runs:

| Where your program runs | Bootstrap server |
|---|---|
| On your own machine (these Python scripts, a Flink job run from your IDE) | `localhost:9092` |
| In another Docker container on the same compose network (e.g. a Flink container) | `kafka:9093` |

Topics are created automatically the first time something writes to them.

## Useful commands

```sh
docker compose ps                       # is Kafka running?
docker compose logs -f kafka            # Kafka logs

# list topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# read a topic from the beginning without Python
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic sensors --from-beginning
```

## Troubleshooting

- **`Cannot connect to the Docker daemon`** – start Docker Desktop and retry.
- **`port is already allocated` / port 9092 in use** – another Kafka (or an old container) is running. Run `./stop.sh`, or stop the other process.
- **The consumer prints nothing** – check that the producer is running and that you used the right topic name. If you started several consumers, stop the extras: consumers share the group `my_consumer_group` and the topic has one partition, so only one of them receives messages.
- **The consumer shows old messages only / replays everything** – it starts from the earliest message by design (`auto_offset_reset='earliest'`) and does not save its position, so each run reads the topic from the start.
- **`NoBrokersAvailable`** – Kafka is not up yet or is stopped. Run `docker compose ps` and wait until it is `healthy`.
- **Start completely fresh** – `./stop.sh && ./start.sh`.
