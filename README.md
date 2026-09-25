# TTC-Mesh: Production Cloud-Native Transit Observability & Service Mesh 🚊 ⚡ ☁️

An enterprise, event-driven Toronto Transit Commission (TTC) monitoring pipeline instrumented with the **CNCF Observability Trinity**:
- **Traces:** CNCF Jaeger & OpenTelemetry (OTel)
- **Metrics:** CNCF Prometheus
- **Logs:** Grafana Loki
- **Visuals:** Auto-provisioned Grafana Dashboards + Real-Time WebSocket Map
- **Service Mesh:** CNCF Meshery + Istio (mTLS & Canary Routing)
- **Isolated Cloud Execution:** Docker Sandboxes (MicroVMs)

```
        [ Live Toronto Transit Commission (TTC) GTFS-RT Protobuf Feed ]
                                      │
                                      ▼
                 ┌─────────────────────────────────────────┐
                 │        ttc-collector (OTel Traced)      │  ──▶ :8001/metrics
                 └────────────────────┬────────────────────┘
                                      │ In-Memory Caching (Redis Pipeline)
                                      ▼
                         ┌────────────────────────┐
                         │   Redis 7 Alpine Cache │
                         └────────────┬───────────┘
                                      │
                                      ▼
                 ┌─────────────────────────────────────────┐
                 │       ttc-api (FastAPI + OTel)          │  ──▶ :8000/ (Dark Live Map)
                 └────────────────────┬────────────────────┘  ──▶ :8000/metrics
                                      │                       ──▶ :8000/graphql
                                      │                       ──▶ :8000/ws/transit
                  ┌───────────────────┼───────────────────┐
                  ▼                   ▼                   ▼
   ┌───────────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐
   │    CNCF Jaeger        │ │ CNCF Prometheus │ │       Grafana Loki      │
   │ (Distributed Traces)  │ │    (Metrics)    │ │   (Log Aggregation)     │
   │       :16686          │ │      :9090      │ │         :3100           │
   └──────────────┬────────┘ └────────┬────────┘ └────────────┬────────────┘
                  │                   │                       │
                  └───────────────────┼───────────────────────┘
                                      ▼
                       ┌─────────────────────────────┐
                       │    Grafana Command Center   │  ──▶ :3000 (Auto-provisioned)
                       └─────────────────────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │     CNCF Meshery + Istio    │  ──▶ :9081 (Canary & mTLS)
                       └─────────────────────────────┘
```

---

## ⚡ Quick Start: Spin Up the Full Stack in 30 Seconds

Ensure OrbStack (or Docker Desktop) is running on your Mac, then run:

```bash
docker compose up -d
```

### 🛰️ Live Services & Port Map:
| Service | URL | Role in the System |
| :--- | :--- | :--- |
| **Toronto Radar Map** | [http://localhost:8000](http://localhost:8000) | Dark-mode live map powered by bidirectional WebSockets |
| **Jaeger Tracing UI** | [http://localhost:16686](http://localhost:16686) | Deep trace inspection: spans for Redis, Protobuf parsing, HTTP |
| **Grafana Command Center** | [http://localhost:3000](http://localhost:3000) | Unified dashboards (`theinfraguy` / `theinfraguy`) |
| **Prometheus Telemetry** | [http://localhost:9090](http://localhost:9090) | PromQL query engine and target scraper |
| **GraphQL Playground** | [http://localhost:8000/graphql](http://localhost:8000/graphql) | Strongly typed query interface with zero over-fetching |
| **FastAPI Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive REST API documentation with `X-Trace-Id` headers |

---

## 🔍 How Distributed Tracing Works

Every API call automatically generates a distributed trace and returns its Trace ID in the HTTP response headers and JSON payload:

```bash
curl -i http://localhost:8000/api/transit/stats
```
**Response Header:**
```http
HTTP/1.1 200 OK
X-Trace-Id: 4bf92f3577b34da6a3ce929d0e0e4736
X-Jaeger-Trace-URL: http://localhost:16686/trace/4bf92f3577b34da6a3ce929d0e0e4736
```
**Response JSON:**
```json
{
  "status": "online",
  "city": "Toronto",
  "active_vehicles": 854,
  "telemetry": {
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "jaeger_url": "http://localhost:16686/trace/4bf92f3577b34da6a3ce929d0e0e4736"
  }
}
```

Open that URL directly in Jaeger to inspect microsecond spans across:
1. FastApi route handler invocation
2. Redis query execution time
3. Upstream serialization & Protobuf parsing

---

---

## 🚍 Real-Time Fleet Scale: ~1,600+ Physical Vehicles

TTC-Mesh connects directly to the official **City of Toronto / TTC BusTime GTFS-RT feed** (`https://bustime.ttc.ca/gtfsrt/vehicles`).

* **Canada's Largest Surface Transit System:** At any peak or midday hour, the Toronto Transit Commission operates between **1,500 and 1,700 active buses and streetcars** across 140+ routes covering the Greater Toronto Area.
* **Binary Protocol Buffers (Protobuf):** The `ttc-collector` ingests dense Google Protocol Buffer payloads every 10–15 seconds, parsing ~1,600 vehicle GPS coordinates, bearings, and speeds in **~0.32 seconds**.
* **Real-World Fleet Dynamics:**
  * **~500–650 vehicles** are actively in motion (`speed > 0 km/h`) cruising along major arterials (Yonge, Bloor, Queen, King, Eglinton, Gardiner Expressway).
  * The remainder are dwell-stopped at passenger boarding stops, transfer terminals, or traffic intersections.
* **Sub-Millisecond Read Pipeline:** Telemetry is cached into Redis 7 hash maps (`ttc:vehicles`) and broadcast over Redis Pub/Sub (`ttc:broadcast`) to connected WebSocket clients.

---

## 🎯 How to Observe Live Vehicle Motion

> [!IMPORTANT]
> **Do NOT press `Cmd + Shift + R` (or browser Refresh)!**
> Pressing `Cmd + Shift + R` forces the browser to discard the active WebSocket connection, wipe the Leaflet map state, and reset all markers to initial coordinates.

### How To Watch Buses Move in Real Time:
1. **Leave the Browser Tab Open:** Keep [http://localhost:8000](http://localhost:8000) open. The `⚡ LIVE` indicator in the header flashes green every 10 seconds as new GPS coordinates stream in.
2. **Smooth CSS Gliding:** Markers use CSS transition interpolation (`transition: transform 1.2s cubic-bezier(0.25, 1, 0.5, 1)`). When a bus advances 50–100 meters, its cyan marker glides along the street.
3. **One-Click Camera Lock:** Click the **`🎯 ZOOM TO MOVING BUS`** button on the HUD card:
   * The camera will automatically swoop (`map.flyTo`) into street level (zoom 16) focused on an active bus traveling at speed.
   * The vehicle popup opens showing its route and velocity.
   * As each 10-second GTFS-RT tick arrives, the map auto-pans and follows the vehicle along Toronto roadways!
   * Click the button again to cycle to the next moving bus across town.

---

## 🕸️ CNCF Meshery: Local vs. Docker Sandbox MicroVM

A common question: *Do I need the Docker Agent VM to use Meshery?*

**NO! Meshery runs 100% locally on your Mac Mini.**

| Mode | Where It Runs | How to Run | Use Case |
| :--- | :--- | :--- | :--- |
| **Local Mode (Default)** | On your Mac Mini via OrbStack / Docker Engine | `brew install mesheryctl`<br>`mesheryctl system start --platform docker` | Day-to-day visual design, canary routing, local Istio mesh inspection at `http://localhost:9081`. |
| **Docker Sandboxes (Cloud MicroVM)** | Remote Docker Agentic Platform | `sbx create`<br>`sbx exec "docker compose up -d"` | Remote CI/CD validation, autonomous AI agent workflows, and sharing live sandbox environments with remote reviewers. |

### Running Meshery Locally in 2 Commands:
```bash
# 1. Install CLI
brew install mesheryctl

# 2. Launch Meshery UI locally on your Mac Mini
mesheryctl system start --platform docker
```
Access the Meshery Visual Canvas at `http://localhost:9081` and import [`meshery/istio-ttc-mesh-pattern.yaml`](meshery/istio-ttc-mesh-pattern.yaml).

---

## ☁️ Automated Validation in Docker Sandboxes

Cloud execution and isolated testing via Docker Agentic Platform (`agentic-platform.docker.com`):
```bash
# Provision isolated cloud microVM
sbx create --size small

# Run end-to-end integration suite in microVM
sbx exec "git clone <repo> && cd <repo> && docker compose up -d && python test_pipeline.py"

# Stop sandbox (billed only for seconds used)
sbx stop
```

