import os
import json
import asyncio
import threading
import time
import requests
from typing import List, Optional
import redis.asyncio as aioredis
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import FileResponse, PlainTextResponse
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
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

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
    bearing: Optional[float] = 0.0
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
                bearing=float(v.get("bearing", 0.0)),
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
                            sorted_veh = sorted(vehicles, key=lambda x: x.get("speed", 0), reverse=True)
                            payload = {
                                "type": "FLEET_UPDATE",
                                "active_vehicles": len(vehicles),
                                "moving_vehicles": speed_count,
                                "avg_speed": avg_speed,
                            "timestamp": curr_sync,
                            "full": True,
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
def _push_loki(payload: dict):
    try:
        requests.post("http://loki:3100/loki/api/v1/push", json=payload, timeout=0.8)
    except Exception:
        pass


@app.middleware("http")
async def add_trace_header(request: Request, call_next):
    response: Response = await call_next(request)
    current_span = trace.get_current_span()
    if current_span:
        trace_id = format(current_span.get_span_context().trace_id, "032x")
        response.headers["X-Trace-Id"] = trace_id
        response.headers["X-Jaeger-Trace-URL"] = f"{JAEGER_PUBLIC_URL}/trace/{trace_id}"
        if not request.url.path.startswith("/metrics") and trace_id and set(trace_id) != {"0"}:
            loki_payload = {
                "streams": [
                    {
                        "stream": {
                            "app": "ttc-api",
                            "level": "info",
                            "service_name": "ttc-api",
                        },
                        "values": [[
                            str(time.time_ns()),
                            f"{request.method} {request.url.path} status={response.status_code} | trace_id={trace_id}",
                        ]],
                    }
                ]
            }
            threading.Thread(target=_push_loki, args=(loki_payload,), daemon=True).start()
    return response

# Mount GraphQL router
app.include_router(graphql_app, prefix="/graphql")

@app.websocket("/ws/transit")
async def websocket_transit_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        raw_data = r_sync.hgetall("ttc:vehicles")
        all_v = [json.loads(val) for val in raw_data.values()] if raw_data else []
        sorted_v = sorted(all_v, key=lambda x: x.get("speed", 0), reverse=True)[:2000]
        initial_stats = {
            "type": "INITIAL_HANDSHAKE",
            "active_vehicles": int(r_sync.get("ttc:total_count") or len(raw_data)),
            "moving_vehicles": int(r_sync.get("ttc:moving_vehicles") or 0),
            "avg_speed": float(r_sync.get("ttc:avg_speed") or 0.0),
            "timestamp": float(r_sync.get("ttc:last_updated") or 0),
            "full": True,
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
def get_vehicles(route: str = None, limit: int = 2000):
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
# Building Block 21: Meshery Service Mesh Pattern Provider
# -------------------------------------------------------------
@app.get("/meshery/pattern", response_class=PlainTextResponse)
def get_meshery_pattern():
    pattern_path = os.path.join(os.path.dirname(__file__), "meshery", "istio-ttc-mesh-pattern.yaml")
    if os.path.exists(pattern_path):
        with open(pattern_path, "r") as f:
            return f.read()
    return "# Meshery pattern not found"

@app.get("/")
def live_command_center():
    index = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html")
    return FileResponse(index, headers={"Cache-Control": "no-cache"})
