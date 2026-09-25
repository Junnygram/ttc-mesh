# System Design Architecture: TTC-Mesh 🚊 📐

This project is a living, production-grade implementation of the **Core System Design Building Blocks**, fully instrumented with the **CNCF Observability Trinity** (Prometheus, Grafana, Loki, Jaeger) and managed via **CNCF Meshery** and **Docker Sandboxes**.

---

## The 20 Building Blocks Mapped to TTC-Mesh

### 1. Client-Server Architecture (3-Tier)
* **Tier 1 (Client):** Interactive Leaflet Map UI and browser clients.
* **Tier 2 (Application Server):** FastAPI (`main.py`) handling business logic and transformations.
* **Tier 3 (Database):** Redis 7 in-memory cache and state store.

### 2. Horizontal Scaling (`--scale`)
* Handled via Docker Compose and Kubernetes ReplicaSets:
  ```bash
  docker compose up -d --scale api=3
  ```
* Requests are load-balanced across multiple API worker instances.

### 3. Databases & 7. Non-Relational (NoSQL)
* **Redis Key-Value & Hashes:** Active vehicles stored as hash maps (`HSET ttc:vehicles <id> <data>`) for sub-millisecond retrieval.

### 4. ACID vs. BASE
* **BASE (Basically Available, Soft State, Eventual Consistency):**
  * Transit telemetry values availability over strict consistency. If a GPS coordinate drops for 2 seconds, the system stays online and updates on the next cycle rather than locking the database.

### 5. CAP Theorem (AP Lean)
* During network partitions, TTC-Mesh chooses **Availability & Partition Tolerance (AP)**. Stale vehicle positions are served gracefully from cache rather than rejecting client requests.

### 8. Database Replication & Caching
* **Read-Heavy Architecture:** Vehicle ingestion is 1 write every 15s; client reads are hundreds per second. Redis acts as a high-throughput read cache decoupling the TTC upstream API.

### 10. HTTP Protocol & Status Codes
* FastAPI enforces strict HTTP semantics:
  * `200 OK` for valid queries.
  * `404 Not Found` for invalid route IDs.
  * `503 Service Unavailable` if Redis health check fails.

### 11. Monolith vs. 12. Microservices
* **Microservices Architecture:**
  * `ttc-collector`: Dedicated ingestion worker.
  * `ttc-api`: Stateless REST/GraphQL/WebSocket server.
  * `redis`: State storage.
  * `jaeger`: Distributed tracing engine.
  * `prometheus`: Telemetry scraper.
  * `grafana`: Visualization layer.
  * `loki`: Log aggregator.

### 13. API Gateway & Service Mesh (Istio + Meshery)
* Managed by **CNCF Meshery** with **Istio Service Mesh** (`meshery/istio-ttc-mesh-pattern.yaml`):
  * Mutual TLS (`ISTIO_MUTUAL`).
  * 90/10 Canary traffic routing between v1 and v2.
  * Envoy circuit breakers and rate limiting.

### 14. REST APIs
* Fully documented via OpenAPI/Swagger specification at `http://localhost:8000/docs`.

### 16. GraphQL (`/graphql`)
* Powered by **Strawberry GraphQL**:
  * Eliminates **over-fetching**: Clients request only the specific fields needed (e.g., `{ vehicles { id speed } }`).
  * Eliminates **under-fetching**: Combines fleet stats and vehicle lists in a single round-trip.

### 17. WebSockets vs. 19. Polling
* **WebSockets (`/ws/transit`):** A persistent, bi-directional connection.
* Connected browsers receive real-time vehicle updates pushed directly from Redis Pub/Sub without polling the server every few seconds.

### 18. gRPC & Protobuf
* **Protocol Buffers:** Upstream TTC live telemetry is consumed natively using Google Protocol Buffers (`gtfs_realtime_pb2.FeedMessage`), providing dense binary serialization and high performance.

### 20. Webhooks (Push-Based Events)
* External services subscribe via `POST /api/webhooks/subscribe`.
* When a high-severity traffic congestion event occurs (e.g. >100 stalled vehicles), the collector automatically pushes an HTTP POST alert to all registered subscriber URLs.

---

## 🔍 Distributed Tracing & Observability (CNCF Standard)
* **OpenTelemetry (OTel):** Both the `api` and `collector` are instrumented with OpenTelemetry SDKs, reporting to **CNCF Jaeger** (`:16686`).
* **Trace Propagation:** Every HTTP request response returns:
  * `X-Trace-Id` header.
  * `X-Jaeger-Trace-URL` header.
  * A `telemetry.jaeger_url` field in the response JSON.
* **Grafana Integration:** Grafana is configured with Prometheus (Metrics), Loki (Logs), and Jaeger (Traces) in a single pane of glass.

---

## 📈 Fleet Scale & Real-Time Data Flow

```
[ TTC BusTime GTFS-RT Protobuf ] (~1,600+ vehicles every 10s)
               │
               ▼  (~0.32s binary decode)
        [ ttc-collector ]
               │
         ┌─────┴─────────────────────┐
         ▼                           ▼
[ Redis Hashes: ttc:vehicles ]  [ Redis Pub/Sub: ttc:broadcast ]
(Sub-ms state caching)                 │ (Fan-out)
                                       ▼
                              [ ttc-api WebSocket Hub ]
                                       │ (Push)
                                       ▼
                       [ Browser Leaflet Canvas ]
             (Persistent DOM nodes + CSS transform interpolation)
```

1. **Volume & Ingestion Velocity:**
   * Ingests ~1,600+ physical buses and streetcars currently active across Toronto's 140+ routes.
   * Feeds decode in ~320ms into structured entity maps.
2. **State Management & Fan-Out:**
   * Caches full vehicle metadata in Redis Hashes (`ttc:vehicles`) for fast REST and GraphQL resolution.
   * Broadcasts high-velocity moving vehicles over Redis Pub/Sub to all connected WebSocket clients.
3. **Client-Side Motion Interpolation:**
   * Rather than re-rendering map layers on each tick (which causes flicker), Leaflet maintains a persistent vehicle Map.
   * When new coordinates arrive, existing DOM markers update via `setLatLng([lat, lon])` with hardware-accelerated CSS `transform` transitions.
   * *Architectural Note on Page Reloads:* A browser hard reload (`Cmd + Shift + R`) reinitializes the WebSocket and wipes marker state, whereas keeping the tab open allows continuous, fluid motion tracking.

---

## ☁️ Cloud Execution: Docker Sandboxes vs. Local Meshery
* **Local Mesh Execution (OrbStack):** CNCF Meshery and the 7-container stack run directly on the host machine using OrbStack, providing immediate local feedback at zero cost.
* **Remote MicroVM Sandboxes:** Docker Sandboxes allow ephemeral cloud microVMs to execute tests autonomously in CI/CD without polluting developer environments.

