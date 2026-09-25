import os
import json
import asyncio
from typing import List, Optional
import redis.asyncio as aioredis
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator
import strawberry
from strawberry.fastapi import GraphQLRouter

# OpenTelemetry & Jaeger Distributed Tracing
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
JAEGER_PUBLIC_URL = os.getenv("JAEGER_PUBLIC_URL", "http://localhost:16686")

# Initialize OpenTelemetry Tracer for API
resource = Resource.create({"service.name": "ttc-api", "service.version": "3.0.0", "deployment.environment": "production"})
provider = TracerProvider(resource=resource)
try:
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
    provider.add_span_processor(processor)
except Exception as e:
    print(f"[OTel] Note: OTLP exporter init deferred: {e}")
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("ttc-api")

# Auto-instrument Redis
RedisInstrumentor().instrument()

# Synchronous Redis for standard REST operations
r_sync = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# -------------------------------------------------------------
# Building Block 16: GraphQL Schema & Types (Strawberry GraphQL)
# -------------------------------------------------------------
@strawberry.type
class VehicleType:
    id: str
    route: str
    lat: float
    lon: float
    speed: float
    timestamp: int

@strawberry.type
class TransitStatsType:
    active_vehicles: int
    last_sync_timestamp: float
    status: str
    trace_id: str

@strawberry.type
class Query:
    @strawberry.field
    def stats(self) -> TransitStatsType:
        current_span = trace.get_current_span()
        trace_id = format(current_span.get_span_context().trace_id, "032x") if current_span else "N/A"
        last_updated = r_sync.get("ttc:last_updated")
        total_count = r_sync.get("ttc:total_count")
        return TransitStatsType(
            active_vehicles=int(total_count) if total_count else 0,
            last_sync_timestamp=float(last_updated) if last_updated else 0.0,
            status="online",
            trace_id=trace_id
        )

    @strawberry.field
    def vehicles(self, route: Optional[str] = None, limit: int = 50) -> List[VehicleType]:
        raw_data = r_sync.hgetall("ttc:vehicles")
        if not raw_data:
            return []
        
        parsed = [json.loads(val) for val in raw_data.values()]
        if route:
            parsed = [v for v in parsed if v.get("route") == route]
            
        return [
            VehicleType(
                id=str(v["id"]),
                route=str(v.get("route", "Unknown")),
                lat=float(v.get("lat", 0.0)),
                lon=float(v.get("lon", 0.0)),
                speed=float(v.get("speed", 0.0)),
                timestamp=int(v.get("timestamp", 0))
            )
            for v in parsed[:limit]
        ]

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

from contextlib import asynccontextmanager

# -------------------------------------------------------------
# Building Block 17: WebSockets & Pub/Sub Connection Manager
# -------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

# Background Broadcaster (PubSub with resilient polling fallback)
async def broadcast_loop():
    last_sync = 0.0
    while True:
        try:
            r_async = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            pubsub = r_async.pubsub()
            await pubsub.subscribe("ttc:broadcast")
            print("[Broadcaster] Subscribed to ttc:broadcast successfully.")
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "message":
                    await manager.broadcast(message["data"])
                else:
                    curr_sync = float(r_sync.get("ttc:last_updated") or 0.0)
                    if curr_sync > last_sync and manager.active_connections:
                        last_sync = curr_sync
                        raw_data = r_sync.hgetall("ttc:vehicles")
                        if raw_data:
                            vehicles = [json.loads(val) for val in raw_data.values()]
                            speed_count = int(r_sync.get("ttc:moving_vehicles") or 0)
                            avg_speed = float(r_sync.get("ttc:avg_speed") or 0.0)
                            sorted_veh = sorted(vehicles, key=lambda x: x.get("speed", 0), reverse=True)[:600]
                            payload = {
                                "type": "FLEET_UPDATE",
                                "active_vehicles": len(vehicles),
                                "moving_vehicles": speed_count,
                                "avg_speed": avg_speed,
                                "timestamp": curr_sync,
                                "vehicles": sorted_veh
                            }
                            await manager.broadcast(json.dumps(payload))
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"[Broadcaster Error] {e}")
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(broadcast_loop())
    yield
    task.cancel()

# -------------------------------------------------------------
# FastAPI Core App Initialization
# -------------------------------------------------------------
app = FastAPI(
    title="TTC-Mesh: Enterprise Observability & System Design",
    description="Toronto Transit live monitoring with Prometheus, Grafana, Jaeger Distributed Tracing, Istio Mesh patterns, and Docker Sandboxes.",
    version="3.1.0",
    lifespan=lifespan
)

# Instrument FastAPI with OpenTelemetry & Prometheus
FastAPIInstrumentor.instrument_app(app)
Instrumentator().instrument(app).expose(app)

# Inject X-Trace-Id header into every response
@app.middleware("http")
async def add_trace_header(request: Request, call_next):
    response: Response = await call_next(request)
    current_span = trace.get_current_span()
    if current_span:
        trace_id = format(current_span.get_span_context().trace_id, "032x")
        response.headers["X-Trace-Id"] = trace_id
        response.headers["X-Jaeger-Trace-URL"] = f"{JAEGER_PUBLIC_URL}/trace/{trace_id}"
    return response

# Mount GraphQL router
app.include_router(graphql_app, prefix="/graphql")

@app.websocket("/ws/transit")
async def websocket_transit_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        raw_data = r_sync.hgetall("ttc:vehicles")
        all_v = [json.loads(val) for val in raw_data.values()] if raw_data else []
        sorted_v = sorted(all_v, key=lambda x: x.get("speed", 0), reverse=True)[:600]
        initial_stats = {
            "type": "INITIAL_HANDSHAKE",
            "active_vehicles": int(r_sync.get("ttc:total_count") or len(raw_data)),
            "moving_vehicles": int(r_sync.get("ttc:moving_vehicles") or 0),
            "avg_speed": float(r_sync.get("ttc:avg_speed") or 0.0),
            "timestamp": float(r_sync.get("ttc:last_updated") or 0),
            "vehicles": sorted_v
        }
        await websocket.send_text(json.dumps(initial_stats))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# -------------------------------------------------------------
# Building Block 20: Webhooks Registration
# -------------------------------------------------------------
class WebhookSubscription(BaseModel):
    url: str
    event_types: List[str] = ["TRAFFIC_CONGESTION_ALERT", "SUBWAY_DELAY"]

@app.post("/api/webhooks/subscribe")
def register_webhook(sub: WebhookSubscription):
    r_sync.sadd("ttc:webhooks", sub.url)
    return {"status": "subscribed", "url": sub.url, "events": sub.event_types}

@app.get("/api/webhooks")
def list_webhooks():
    return {"registered_webhooks": list(r_sync.smembers("ttc:webhooks"))}

# -------------------------------------------------------------
# Building Block 15: REST API Endpoints with Trace ID Response
# -------------------------------------------------------------
@app.get("/api/transit/stats")
def get_stats():
    current_span = trace.get_current_span()
    trace_id = format(current_span.get_span_context().trace_id, "032x") if current_span else "N/A"
    last_updated = r_sync.get("ttc:last_updated")
    total_count = r_sync.get("ttc:total_count")
    avg_speed = float(r_sync.get("ttc:avg_speed") or 0.0)
    moving_vehicles = int(r_sync.get("ttc:moving_vehicles") or 0)
    return {
        "status": "online",
        "city": "Toronto",
        "active_vehicles": int(total_count) if total_count else 0,
        "moving_vehicles": moving_vehicles,
        "avg_speed": avg_speed,
        "last_sync_timestamp": float(last_updated) if last_updated else 0,
        "telemetry": {
            "trace_id": trace_id,
            "jaeger_url": f"{JAEGER_PUBLIC_URL}/trace/{trace_id}"
        }
    }

@app.get("/api/transit/vehicles")
def get_vehicles(route: str = None, limit: int = 100):
    current_span = trace.get_current_span()
    trace_id = format(current_span.get_span_context().trace_id, "032x") if current_span else "N/A"
    raw_data = r_sync.hgetall("ttc:vehicles")
    if not raw_data:
        return {"data": [], "trace_id": trace_id}
    
    vehicles = [json.loads(val) for val in raw_data.values()]
    if route:
        vehicles = [v for v in vehicles if v.get("route") == route]
        
    return {
        "count": min(len(vehicles), limit),
        "trace_id": trace_id,
        "jaeger_url": f"{JAEGER_PUBLIC_URL}/trace/{trace_id}",
        "vehicles": vehicles[:limit]
    }

# -------------------------------------------------------------
# Frontend: Real-Time Dark Mode Map with WebSockets & Jaeger Link
# -------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def live_command_center():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>TTC-Mesh // CNCF Observability Command Center</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg: #070a10;
                --panel: rgba(13, 20, 36, 0.85);
                --accent: #00f0ff;
                --accent-dim: rgba(0, 240, 255, 0.12);
                --danger: #ff0055;
                --success: #00ff88;
                --purple: #a855f7;
                --jaeger: #f97316;
                --text: #f1f5f9;
                --text-muted: #64748b;
                --border: rgba(255, 255, 255, 0.08);
            }
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body {
                background: var(--bg);
                color: var(--text);
                font-family: 'JetBrains Mono', -apple-system, monospace;
                height: 100vh;
                display: flex;
                flex-direction: column;
                overflow: hidden;
            }
            header {
                background: var(--panel);
                backdrop-filter: blur(14px);
                border-bottom: 1px solid var(--border);
                padding: 8px 16px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                z-index: 1000;
                flex-wrap: nowrap;
                overflow-x: auto;
            }
            .brand {
                display: flex;
                align-items: center;
                gap: 8px;
                flex-shrink: 0;
            }
            .logo-badge {
                background: linear-gradient(135deg, #00f0ff, #8b5cf6);
                color: #050811;
                font-weight: 800;
                padding: 3px 8px;
                border-radius: 5px;
                font-size: 12px;
                letter-spacing: 0.5px;
            }
            .brand-sub {
                font-size: 11px;
                color: var(--text-muted);
                letter-spacing: 0.5px;
            }
            .pill-status {
                display: inline-flex;
                align-items: center;
                gap: 5px;
                font-size: 10px;
                padding: 2px 7px;
                border-radius: 20px;
                background: rgba(0, 255, 136, 0.1);
                color: var(--success);
                border: 1px solid rgba(0, 255, 136, 0.3);
                font-weight: 600;
                white-space: nowrap;
            }
            .pill-dot {
                width: 6px;
                height: 6px;
                border-radius: 50%;
                background: var(--success);
                box-shadow: 0 0 6px var(--success);
            }
            .nav-links {
                display: flex;
                gap: 6px;
                align-items: center;
                flex-shrink: 0;
            }
            .btn {
                background: rgba(255, 255, 255, 0.04);
                color: var(--text);
                border: 1px solid var(--border);
                padding: 4px 8px;
                border-radius: 5px;
                font-size: 11px;
                font-weight: 600;
                text-decoration: none;
                transition: all 0.15s ease;
                display: inline-flex;
                align-items: center;
                gap: 4px;
                white-space: nowrap;
            }
            .btn:hover {
                background: var(--accent);
                color: #000;
                border-color: var(--accent);
                transform: translateY(-1px);
            }
            .btn-jaeger { color: #fdba74; border-color: rgba(249, 115, 22, 0.3); background: rgba(249, 115, 22, 0.1); }
            .btn-graf { color: #67e8f9; border-color: rgba(6, 182, 212, 0.3); background: rgba(6, 182, 212, 0.1); }
            .btn-prom { color: #fca5a5; border-color: rgba(239, 68, 68, 0.3); background: rgba(239, 68, 68, 0.1); }
            .btn-mesh { color: #93c5fd; border-color: rgba(59, 130, 246, 0.3); background: rgba(59, 130, 246, 0.1); }
            .btn-gql { color: #d8b4fe; border-color: rgba(168, 85, 247, 0.3); background: rgba(168, 85, 247, 0.1); }

            #app-container { display: flex; flex: 1; position: relative; }
            #map { flex: 1; height: 100%; background: #070a10; }

            .hud-card {
                position: absolute;
                top: 14px;
                right: 14px;
                width: 260px;
                background: var(--panel);
                backdrop-filter: blur(14px);
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 14px;
                z-index: 1000;
                box-shadow: 0 16px 32px rgba(0,0,0,0.5);
            }
            .hud-header {
                display: flex;
                justify-content: space-between;
                align-items: baseline;
                margin-bottom: 2px;
            }
            .hud-label {
                font-size: 9px;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: var(--text-muted);
            }
            .hud-val {
                font-size: 28px;
                font-weight: 800;
                color: var(--accent);
                line-height: 1.1;
                margin: 4px 0;
            }
            .hud-sub {
                font-size: 10px;
                color: var(--text-muted);
                margin-bottom: 12px;
            }
            .chip-grid {
                display: flex;
                flex-wrap: wrap;
                gap: 4px;
                margin-top: 8px;
                padding-top: 8px;
                border-top: 1px solid var(--border);
            }
            .chip {
                font-size: 9px;
                padding: 2px 6px;
                border-radius: 3px;
                background: rgba(255, 255, 255, 0.05);
                border: 1px solid var(--border);
                color: #cbd5e1;
            }
            .chip-active {
                color: var(--accent);
                border-color: rgba(0, 240, 255, 0.3);
                background: var(--accent-dim);
            }

            /* Smoothly glide moving vehicles across roads */
            .leaflet-interactive {
                transition: transform 1.2s cubic-bezier(0.25, 1, 0.5, 1) !important;
            }
            .btn-track {
                width: 100%;
                margin-top: 10px;
                background: linear-gradient(135deg, rgba(0, 240, 255, 0.15), rgba(168, 85, 247, 0.15));
                border: 1px solid var(--accent);
                color: var(--accent);
                padding: 8px 10px;
                border-radius: 6px;
                font-size: 11px;
                font-family: inherit;
                font-weight: 700;
                cursor: pointer;
                transition: all 0.2s ease;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
            }
            .btn-track:hover {
                background: var(--accent);
                color: #050811;
                box-shadow: 0 0 14px rgba(0, 240, 255, 0.5);
                transform: translateY(-1px);
            }
            .legend-bar {
                display: flex;
                flex-direction: column;
                gap: 5px;
                margin-top: 10px;
                padding-top: 8px;
                border-top: 1px solid var(--border);
                font-size: 10px;
                color: var(--text-muted);
            }
            .legend-item {
                display: flex;
                align-items: center;
                gap: 6px;
            }
            .legend-dot {
                width: 8px;
                height: 8px;
                border-radius: 50%;
                display: inline-block;
            }
        </style>
    </head>
    <body>
        <header>
            <div class="brand">
                <span class="logo-badge">TTC // MESH</span>
                <span class="brand-sub">TORONTO</span>
                <span class="pill-status" id="ws-badge"><span class="pill-dot"></span>LIVE</span>
            </div>
            <div class="nav-links">
                <a href="http://localhost:16686" target="_blank" class="btn btn-jaeger" title="Distributed Tracing">🔍 JAEGER</a>
                <a href="http://localhost:3000" target="_blank" class="btn btn-graf" title="Metrics Dashboard">📊 GRAF</a>
                <a href="http://localhost:9090" target="_blank" class="btn btn-prom" title="Prometheus Metrics">⚡ PROM</a>
                <a href="http://localhost:9081" target="_blank" class="btn btn-mesh" title="Meshery Service Mesh">🕸️ MESH</a>
                <a href="/graphql" target="_blank" class="btn btn-gql" title="GraphQL Playground">🔮 GQL</a>
                <a href="/docs" target="_blank" class="btn" title="OpenAPI Documentation">📖 API</a>
            </div>
        </header>

        <div id="app-container">
            <div id="map"></div>

            <div class="hud-card">
                <div class="hud-header">
                    <span class="hud-label">FLEET ACTIVE</span>
                    <span style="font-size:9px; color:var(--success);">GTFS-RT</span>
                </div>
                <div class="hud-val" id="metric-active">...</div>
                <div id="metric-moving" style="color:var(--accent); font-weight:700; font-size:11px; margin-bottom:4px;">⚡ Tracking Moving Fleet...</div>
                <div class="hud-sub" id="metric-speed">AVG SPEED: ... km/h</div>

                <div class="hud-label">STACK CODENAMES</div>
                <div class="chip-grid">
                    <span class="chip chip-active">OTel</span>
                    <span class="chip chip-active">Jaeger</span>
                    <span class="chip chip-active">Prom</span>
                    <span class="chip chip-active">Graf</span>
                    <span class="chip chip-active">Loki</span>
                    <span class="chip chip-active">Istio</span>
                    <span class="chip chip-active">Mesh</span>
                    <span class="chip">Redis</span>
                    <span class="chip">WS</span>
                    <span class="chip">GQL</span>
                    <span class="chip">SBX</span>
                </div>

                <button id="btn-track-moving" onclick="trackMovingVehicle()" class="btn-track">🎯 ZOOM TO MOVING BUS</button>
                <div id="tracked-info" style="font-size:10px; color:#facc15; margin-top:8px; line-height:1.4; display:none;"></div>
                
                <div class="legend-bar">
                    <div class="legend-item"><span class="legend-dot" style="background:#00f0ff; box-shadow:0 0 6px #00f0ff;"></span> <span style="color:#f1f5f9;">Moving Bus</span> (speed > 0)</div>
                    <div class="legend-item"><span class="legend-dot" style="background:#475569;"></span> <span>Stopped / Dwell</span> (0 km/h)</div>
                    <div class="legend-item"><span class="legend-dot" style="background:#facc15; box-shadow:0 0 8px #facc15;"></span> <span style="color:#facc15;">Locked Target</span> (Auto-follow)</div>
                </div>

                <div style="font-size:9px; color:var(--text-muted); margin-top:8px; text-align:center;">
                    ⚡ Auto-syncs every 10s. Keep tab open (no reload needed!)
                </div>
            </div>
        </div>

        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script>
            const map = L.map('map', { zoomControl: false }).setView([43.6532, -79.3832], 13);
            L.control.zoom({ position: 'bottomright' }).addTo(map);

            // 100% Free Dark Gray Canvas (No API Key Required)
            L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
                attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ',
                maxZoom: 16
            }).addTo(map);

            // Subtle reference labels & street names overlay
            L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}', {
                maxZoom: 16
            }).addTo(map);

            let markerGroup = L.layerGroup().addTo(map);
            const vehicleMarkers = new Map();
            const wsBadge = document.getElementById('ws-badge');
            let movingVehiclesList = [];
            let trackedVehicleId = null;

            function trackMovingVehicle() {
                if (!movingVehiclesList || movingVehiclesList.length === 0) {
                    alert("Syncing live fleet telemetry... Please wait a few seconds.");
                    return;
                }
                // Pick next moving vehicle
                let target = null;
                if (trackedVehicleId) {
                    const currIdx = movingVehiclesList.findIndex(v => v.id === trackedVehicleId);
                    target = movingVehiclesList[(currIdx + 1) % movingVehiclesList.length];
                } else {
                    target = [...movingVehiclesList].sort((a,b) => b.speed - a.speed)[0];
                }

                if (target) {
                    trackedVehicleId = target.id;
                    map.flyTo([target.lat, target.lon], 16, { animate: true, duration: 1.5 });
                    const marker = vehicleMarkers.get(target.id);
                    if (marker) marker.openPopup();
                    const infoEl = document.getElementById('tracked-info');
                    if (infoEl) {
                        infoEl.style.display = 'block';
                        infoEl.innerHTML = `🔭 Locked on <b>Route ${target.route}</b> (Bus #${target.id})<br>⚡ Speed: <b>${target.speed} km/h</b>. Auto-following...`;
                    }
                }
            }

            function renderVehicles(vehicles) {
                if (!vehicles || vehicles.length === 0) return;
                let inMotion = 0;
                movingVehiclesList = [];

                vehicles.forEach(v => {
                    if (!v.lat || !v.lon) return;

                    const isMoving = v.speed && v.speed > 0;
                    if (isMoving) {
                        inMotion++;
                        movingVehiclesList.push(v);
                    }

                    const isTracked = (v.id === trackedVehicleId);
                    
                    let color, radius, fillOpacity, statusLabel;
                    if (isTracked) {
                        color = "#facc15"; // Radiant Gold for the tracked target
                        radius = 9;
                        fillOpacity = 1.0;
                        statusLabel = `<span style="color:#facc15; font-weight:800;">🎯 LOCKED TARGET</span> (${v.speed || 0} km/h)`;
                    } else if (isMoving) {
                        color = "#00f0ff"; // Electric Cyan for moving vehicles
                        radius = 5.5;
                        fillOpacity = 0.95;
                        statusLabel = `<span style="color:#00f0ff;">⚡ MOVING</span> (${v.speed} km/h)`;
                    } else {
                        color = "#475569"; // Slate Grey for stopped / dwell vehicles
                        radius = 3.5;
                        fillOpacity = 0.55;
                        statusLabel = `<span style="color:#94a3b8;">🛑 STOPPED / DWELL</span>`;
                    }

                    const popupHtml = `
                        <div style="font-size:11px; font-family:'JetBrains Mono',monospace;">
                            <strong style="color:${color};">Route ${v.route}</strong> ${statusLabel}<br>
                            Vehicle ID: ${v.id}<br>
                            Lat: ${v.lat}, Lon: ${v.lon}
                        </div>
                    `;

                    if (vehicleMarkers.has(v.id)) {
                        // Smoothly animate existing marker to new GPS coordinate
                        const marker = vehicleMarkers.get(v.id);
                        marker.setLatLng([v.lat, v.lon]);
                        marker.setStyle({
                            fillColor: color,
                            radius: radius,
                            fillOpacity: fillOpacity,
                            color: isTracked ? "#ffffff" : (isMoving ? "#ffffff" : "#334155"),
                            weight: isTracked ? 3 : (isMoving ? 2 : 1)
                        });
                        marker.setPopupContent(popupHtml);
                    } else {
                        // Create new marker
                        const marker = L.circleMarker([v.lat, v.lon], {
                            radius: radius,
                            fillColor: color,
                            color: isTracked ? "#ffffff" : (isMoving ? "#ffffff" : "#334155"),
                            weight: isTracked ? 3 : (isMoving ? 2 : 1),
                            opacity: 0.9,
                            fillOpacity: fillOpacity
                        });
                        marker.bindPopup(popupHtml);
                        markerGroup.addLayer(marker);
                        vehicleMarkers.set(v.id, marker);
                    }
                });

                // If currently tracking a vehicle, smoothly pan to its new coordinate
                if (trackedVehicleId) {
                    const tracked = vehicles.find(v => v.id === trackedVehicleId);
                    if (tracked) {
                        map.panTo([tracked.lat, tracked.lon], { animate: true, duration: 1.2 });
                        const infoEl = document.getElementById('tracked-info');
                        if (infoEl) {
                            infoEl.innerHTML = `🔭 Following <b>Route ${tracked.route}</b> (Bus #${tracked.id})<br>⚡ Speed: <b>${tracked.speed} km/h</b>. Gliding along road...`;
                        }
                    }
                }

                const motionEl = document.getElementById('metric-moving');
                if (motionEl) {
                    motionEl.innerHTML = `⚡ <span style="color:#00ff88;">${inMotion}</span> Vehicles In Motion`;
                }
            }

            // Immediately load initial batch so map populates without waiting
            function loadVehicles() {
                fetch('/api/transit/vehicles?limit=600')
                    .then(res => res.json())
                    .then(data => {
                        const list = data.vehicles || data;
                        renderVehicles(list);
                    })
                    .catch(e => console.error("Vehicle fetch error:", e));

                fetch('/api/transit/stats')
                    .then(res => res.json())
                    .then(data => {
                        if (data.active_vehicles) document.getElementById('metric-active').textContent = data.active_vehicles;
                        if (data.avg_speed) document.getElementById('metric-speed').textContent = `AVG SPEED: ${data.avg_speed} km/h`;
                        if (data.moving_vehicles) document.getElementById('metric-moving').innerHTML = `⚡ <span style="color:#00ff88;">${data.moving_vehicles}</span> Vehicles In Motion`;
                    })
                    .catch(() => {});
            }

            loadVehicles();

            // Resilient dual-sync: updates every 10s even if browser sleeps WebSocket
            setInterval(loadVehicles, 10000);

            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws/transit`;
            const socket = new WebSocket(wsUrl);

            socket.onopen = () => {
                wsBadge.textContent = "⚡ LIVE WS";
                wsBadge.style.color = "#00ff88";
                wsBadge.style.borderColor = "#00ff88";
            };

            socket.onmessage = (event) => {
                const message = JSON.parse(event.data);
                if (message.active_vehicles !== undefined) {
                    document.getElementById('metric-active').textContent = message.active_vehicles;
                }
                if (message.avg_speed !== undefined) {
                    document.getElementById('metric-speed').textContent = `AVG SPEED: ${message.avg_speed} km/h`;
                }
                if (message.moving_vehicles !== undefined) {
                    document.getElementById('metric-moving').innerHTML = `⚡ <span style="color:#00ff88;">${message.moving_vehicles}</span> Vehicles In Motion`;
                }

                if (message.vehicles && message.vehicles.length > 0) {
                    renderVehicles(message.vehicles);
                    // Flash live badge so user visibly sees the pulse
                    wsBadge.style.boxShadow = "0 0 14px #00ff88";
                    setTimeout(() => { wsBadge.style.boxShadow = "none"; }, 500);
                }
            };

            socket.onclose = () => {
                wsBadge.textContent = "⚠️ OFFLINE";
                wsBadge.style.color = "#ff0055";
                wsBadge.style.borderColor = "#ff0055";
            };
        </script>
    </body>
    </html>
    """
