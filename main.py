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
# Agencies this deployment knows. `live` means a public vehicle feed is polled.
# `key` means the agency publishes GTFS-RT behind a developer key.
# `unavailable` means there is no public vehicle-position feed.
SERVICES = [
    {"id": "ttc", "city": "Toronto", "agency": "TTC", "province": "Ontario", "status": "live", "note": ""},
    {"id": "stm", "city": "Montreal", "agency": "STM", "province": "Quebec", "status": "key", "note": "STM publishes live positions behind a developer key."},
    {"id": "translink", "city": "Vancouver", "agency": "TransLink", "province": "British Columbia", "status": "key", "note": "TransLink publishes live positions behind an API key."},
    {"id": "victoria", "city": "Victoria", "agency": "BC Transit", "province": "British Columbia", "status": "live", "note": ""},
    {"id": "calgary", "city": "Calgary", "agency": "Calgary Transit", "province": "Alberta", "status": "live", "note": ""},
    {"id": "octranspo", "city": "Ottawa", "agency": "OC Transpo", "province": "Ontario", "status": "key", "note": "OC Transpo publishes live positions behind a subscription key."},
    {"id": "edmonton", "city": "Edmonton", "agency": "ETS", "province": "Alberta", "status": "live", "note": ""},
    {"id": "winnipeg", "city": "Winnipeg", "agency": "Winnipeg Transit", "province": "Manitoba", "status": "unavailable", "note": "Winnipeg Transit does not publish a public vehicle feed."},
    {"id": "halifax", "city": "Halifax", "agency": "Halifax Transit", "province": "Nova Scotia", "status": "live", "note": ""},
]
ROUTE_NAMES = {
    "501": "Queen", "502": "Downtowner", "503": "Kingston Rd", "504": "King",
    "505": "Dundas", "506": "Carlton", "509": "Harbourfront", "510": "Spadina",
    "511": "Bathurst", "512": "St Clair", "29": "Dufferin", "35": "Jane",
    "36": "Finch West", "7": "Bathurst", "6": "Bay", "960": "Steeles Express",
    "939": "Finch Express", "52": "Lawrence West", "96": "Wilson",
    "201": "Red Line", "202": "Blue Line",
}


def _load_vehicles():
    raw_data = r_sync.hgetall("ttc:vehicles")
    if not raw_data:
        return []
    return [json.loads(val) for val in raw_data.values()]


def _route_label(route, city=""):
    name = ROUTE_NAMES.get(str(route or ""), "") if city == "Toronto" else ""
    return f"{route} {name}" if name else str(route or "")


def _stop_place(text):
    value = str(text or "")
    for prefix in ("Arriving at ", "Stopped at ", "Next stop "):
        if value.startswith(prefix):
            return value[len(prefix):]
    return value


def _blob_match(vehicle, q):
    route = str(vehicle.get("route") or "")
    city = str(vehicle.get("city") or "")
    name = ROUTE_NAMES.get(route, "") if city == "Toronto" else ""
    label = f"{route} {name}".strip().lower()
    toward = str(vehicle.get("toward") or "")
    stop = str(vehicle.get("stop") or "")
    agency = str(vehicle.get("agency") or "")
    text = q.lower().strip()
    if not text:
        return 0
    if route.lower() == text:
        return 95
    if text.isdigit() and len(text) >= 2 and route.lower().startswith(text):
        return 72
    if name and text in name.lower():
        return 78
    if label.startswith(text):
        return 70
    if toward.lower().startswith(text):
        return 64
    if len(text) >= 3 and text in toward.lower():
        return 50
    if len(text) >= 3 and text in stop.lower():
        return 36
    if len(text) >= 3 and (text in city.lower() or text in agency.lower()):
        return 24
    return 0


@strawberry.type
class VehicleType:
    id: str
    route: str
    lat: float
    lon: float
    speed: float
    bearing: Optional[float] = 0.0
    timestamp: int
    city: str = ""
    agency: str = ""
    province: str = ""
    toward: str = ""
    compass: str = ""
    stop: str = ""


def _as_vehicle(v):
    city = str(v.get("city") or "")
    province = ""
    for service in SERVICES:
        if service["city"] == city:
            province = service["province"]
            break
    return VehicleType(
        id=str(v.get("id") or ""),
        route=str(v.get("route") or "Unknown"),
        lat=float(v.get("lat") or 0.0),
        lon=float(v.get("lon") or 0.0),
        speed=float(v.get("speed") or 0.0),
        bearing=float(v.get("bearing") or 0.0),
        timestamp=int(v.get("timestamp") or 0),
        city=city,
        agency=str(v.get("agency") or ""),
        province=province,
        toward=str(v.get("toward") or ""),
        compass=str(v.get("compass") or ""),
        stop=str(v.get("stop") or ""),
    )


@strawberry.type
class ServiceType:
    id: str
    city: str
    agency: str
    province: str
    status: str
    vehicles: int
    note: str


@strawberry.type
class SearchHit:
    kind: str
    title: str
    subtitle: str
    city: str
    agency: str
    province: str
    route: str
    vehicles: int


def _service_rows():
    counts = {}
    for vehicle in _load_vehicles():
        city = vehicle.get("city") or ""
        counts[city] = counts.get(city, 0) + 1
    rows = []
    for service in SERVICES:
        rows.append(ServiceType(
            id=service["id"],
            city=service["city"],
            agency=service["agency"],
            province=service["province"],
            status=service["status"],
            vehicles=counts.get(service["city"], 0),
            note=service["note"],
        ))
    return rows


def _search_hits(q: str, limit: int):
    text = (q or "").strip().lower()
    if not text:
        return []
    hits = []
    vehicles = _load_vehicles()
    counts = {}
    for vehicle in vehicles:
        counts[vehicle.get("city") or ""] = counts.get(vehicle.get("city") or "", 0) + 1
    for service in SERVICES:
        city = service["city"].lower()
        agency = service["agency"].lower()
        if city.startswith(text) or text in agency:
            hits.append(SearchHit(
                kind="city",
                title=service["city"],
                subtitle=service["agency"] + " · " + service["province"],
                city=service["city"],
                agency=service["agency"],
                province=service["province"],
                route="",
                vehicles=counts.get(service["city"], 0),
            ))
    groups = {}
    for vehicle in vehicles:
        score = _blob_match(vehicle, text)
        if score < 36 or score == 24:
            continue
        route = str(vehicle.get("route") or "")
        toward = str(vehicle.get("toward") or "")
        city = str(vehicle.get("city") or "")
        if score >= 70:
            key = ("route", city, route)
            title = _route_label(route, city)
        elif score >= 50:
            key = ("destination", city, route, toward)
            title = toward
        else:
            place = _stop_place(vehicle.get("stop"))
            key = ("stop", city, place)
            title = place
        current = groups.get(key)
        if current is None:
            groups[key] = {
                "score": score,
                "n": 1,
                "title": title,
                "city": city,
                "agency": str(vehicle.get("agency") or ""),
                "route": route,
                "kind": key[0],
            }
        else:
            current["n"] += 1
            current["score"] = max(current["score"], score)
    for item in groups.values():
        kind = item["kind"]
        count = f"{item['n']} vehicle" + ("" if item["n"] == 1 else "s")
        if kind == "route":
            subtitle = f"{item['agency']} · {count}"
        elif kind == "destination":
            subtitle = f"{item['agency']} · route {item['route']} · {count} this way"
        else:
            subtitle = f"{item['agency']} · {count} near this stop"
            item["route"] = ""
        province = next((s["province"] for s in SERVICES if s["city"] == item["city"]), "")
        hits.append(SearchHit(
            kind=kind,
            title=item["title"],
            subtitle=subtitle,
            city=item["city"],
            agency=item["agency"],
            province=province,
            route=item["route"],
            vehicles=item["n"],
        ))
    def rank(hit):
        title = hit.title.lower()
        exact = 0 if title.startswith(text) else 1
        kind_order = {"city": 0, "route": 1, "destination": 2, "stop": 3}.get(hit.kind, 4)
        return (kind_order, exact, -hit.vehicles)
    hits.sort(key=rank)
    return hits[: max(1, min(limit, 20))]


@strawberry.type
class TransitStatsType:
    active_vehicles: int
    last_sync_timestamp: float
    status: str
    trace_id: str

@strawberry.type
class Query:
    @strawberry.field
    def services(self) -> List[ServiceType]:
        return _service_rows()

    @strawberry.field
    def search(self, q: str, limit: int = 8) -> List[SearchHit]:
        return _search_hits(q, limit)

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
    def vehicles(
        self,
        city: Optional[str] = None,
        agency: Optional[str] = None,
        route: Optional[str] = None,
        q: Optional[str] = None,
        limit: int = 50,
    ) -> List[VehicleType]:
        parsed = _load_vehicles()
        if city:
            parsed = [v for v in parsed if str(v.get("city") or "").lower() == city.lower()]
        if agency:
            parsed = [v for v in parsed if str(v.get("agency") or "").lower() == agency.lower()]
        if route:
            parsed = [v for v in parsed if str(v.get("route") or "") == str(route)]
        if q:
            parsed = [v for v in parsed if _blob_match(v, q)]
            parsed.sort(key=lambda v: _blob_match(v, q), reverse=True)
        cap = max(1, min(int(limit or 50), 5000))
        return [_as_vehicle(v) for v in parsed[:cap]]

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
        sorted_v = sorted(all_v, key=lambda x: x.get("speed", 0), reverse=True)[:8000]
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
        "feed_age_seconds": float(r_sync.get("ttc:feed_age") or 0),
        "telemetry": {
            "trace_id": trace_id,
            "jaeger_url": f"{JAEGER_PUBLIC_URL}/trace/{trace_id}"
        }
    }

@app.get("/api/transit/vehicles")
def get_vehicles(route: str = None, limit: int = 8000):
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
