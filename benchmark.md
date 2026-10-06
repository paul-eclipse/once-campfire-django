# Benchmarks

Instructions for running the performance benchmarks using Docker and the automated runner.

---

## Prerequisites

Install the benchmark runner dependencies:

```sh
pip install aiohttp websockets
```

---

## 1. Build the Production Image

```sh
docker build --build-arg REVISION="$(git rev-parse HEAD)" -t once-campfire-django:benchmark .
```

---

## 2. Start the Production Server

Run the container pinned to 4 CPU threads with an ephemeral secret key:

```sh
docker run --rm -it --cpus="4" -p 8080:80 -e SECRET_KEY_BASE="benchmark-secret-key-base-0123456789abcdef" once-campfire-django:benchmark
```

---

## 3. Run the Benchmark Suite

In a separate terminal, execute the automated benchmark runner:

```sh
python run_benchmarks.py
```

The script automatically initializes the admin account on first run, validates authentication, executes all HTTP and WebSocket workloads, and outputs the performance report.

### Options (Optional)

| Option | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1:8080` | Target server address (`host:port`) |
| `--email` | `admin@example.com` | Email address for automatic authentication login |
| `--password` | `secret` | Password for automatic authentication login |
| `--cookie` | `""` | Authenticated session cookie string (`_campfire_session=...; session_token=...`) |
| `--room-id` | `1` | Target room ID for benchmark requests |
| `--duration` | `15` | Test duration (in seconds) per HTTP scenario |
| `--concurrency` | `16` | Number of concurrent HTTP worker clients |
| `--clients` | `100` | Number of WebSocket clients for Action Cable test |
| `--messages` | `30` | Number of broadcast messages to inject |
| `--rate` | `5.0` | Broadcast message injection rate (messages/sec) |
| `--output` | `tmp/benchmark_report.md` | Path to save the Markdown benchmark report |
| `--skip-http` | `False` | Skip HTTP concurrency tests |
| `--skip-ws` | `False` | Skip Action Cable WebSocket real-time tests |

---

## 4. Stop the Production Server

Press `Ctrl+C` in the terminal where the container is running to stop and automatically remove the container.
