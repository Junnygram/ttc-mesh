import os
import time
import json
import math
import requests
import redis
from google.transit import gtfs_realtime_pb2
from prometheus_client import start_http_server, Gauge, Counter, Histogram

# OpenTelemetry & Jaeger Tracing
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from headsigns import direction_for, ensure_loaded, stop_name
from schedules import ensure_loaded as ensure_calgary
from schedules import trip as calgary_trip

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
METRICS_PORT = int(os.getenv("METRICS_PORT", 8001))
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
TTC_FEED_URL = "https://bustime.ttc.ca/gtfsrt/vehicles"
TTC_ALERTS_URL = "https://bustime.ttc.ca/gtfsrt/alerts"
FEED_HEADERS = {"User-Agent": "ttc-mesh"}
# Open vehicle-position feeds. STM, TransLink, and OC Transpo publish the
# same GTFS-RT shape behind a developer key, so those cities stay on the map
# as camera stops until a key is configured.
AGENCIES = [
    {"id": "ttc", "city": "Toronto", "name": "TTC", "url": TTC_FEED_URL},
    {
        "id": "calgary",
        "city": "Calgary",
        "name": "Calgary Transit",
        "url": "https://data.calgary.ca/download/am7c-qe3u/application%2Foctet-stream",
    },
    {
        "id": "edmonton",
        "city": "Edmonton",
        "name": "ETS",
        "url": "http://gtfs.edmonton.ca/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb",
    },
    {
        "id": "halifax",
        "city": "Halifax",
        "name": "Halifax Transit",
        "url": "http://gtfs.halifax.ca/realtime/Vehicle/VehiclePositions.pb",
    },
    {
        "id": "victoria",
        "city": "Victoria",
        "name": "BC Transit",
        "url": "https://bct.tmix.se/gtfs-realtime/vehicleupdates.pb?operatorIds=48",
    },
]
HELD = {}
COMPASS_WORDS = ["North", "Northeast", "East", "Southeast", "South", "Southwest", "West", "Northwest"]
OCCUPANCY = {
    2: "Few seats",
    3: "Standing room",
    4: "Packed",
    5: "Full",
    6: "Not boarding",
}
STOP_STATUS = {0: "Arriving at", 1: "Stopped at", 2: "Next stop"}
_ALERTS = {}
_ALERTS_AT = 0.0

# Setup OpenTelemetry Tracer for Collector
resource = Resource.create({"service.name": "ttc-collector", "service.version": "3.0.0", "deployment.environment": "production"})
provider = TracerProvider(resource=resource)
try:
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
    provider.add_span_processor(processor)
except Exception as e:
    print(f"[OTel] Note: OTLP exporter init deferred: {e}")
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("ttc-collector")

# Auto-instrument outbound HTTP. Redis pipelines are traced by the
# redis_pipeline_cache span; instrumenting every HSET would name one span
# after all 1,500 commands.
RequestsInstrumentor().instrument()

# Prometheus Metrics
ACTIVE_VEHICLES_GAUGE = Gauge("ttc_vehicles_active", "Number of currently active TTC surface vehicles", ["route_type"])
SYNC_DURATION_HISTOGRAM = Histogram("ttc_collector_sync_duration_seconds", "Duration of TTC GTFS-RT feed polling and parsing")
SYNC_ERRORS_COUNTER = Counter("ttc_collector_sync_errors_total", "Total sync errors from TTC API")
VEHICLES_SYNCED_COUNTER = Counter("ttc_vehicles_synced_total", "Cumulative number of vehicle records processed")
AVG_SPEED_GAUGE = Gauge("ttc_fleet_avg_speed_kmh", "Average current speed of the active TTC surface fleet")
MOVING_VEHICLES_GAUGE = Gauge("ttc_vehicles_moving", "Vehicles currently reporting a non-zero speed")
LAST_SYNC_GAUGE = Gauge("ttc_collector_last_sync_duration_seconds", "Duration of the most recent GTFS-RT sync")
FEED_AGE_GAUGE = Gauge("ttc_feed_age_seconds", "Age of the newest vehicle position in the TTC feed")
FULL_VEHICLES_GAUGE = Gauge("ttc_vehicles_full", "Vehicles reporting full or not boarding")
BUNCHED_VEHICLES_GAUGE = Gauge("ttc_vehicles_bunched", "Vehicles riding within 400m of another on the same trip direction")
ALERTS_GAUGE = Gauge("ttc_service_alerts_active", "Distinct routes named in the current TTC service alerts")

# Route gauges stay bounded by the number of routes. Per-vehicle series do not.
ROUTE_VEHICLES_GAUGE = Gauge("ttc_route_active_vehicles", "Active vehicles per TTC route", ["route", "mode"])
ROUTE_SPEED_GAUGE = Gauge("ttc_route_avg_speed_kmh", "Average speed per TTC route in km/h", ["route", "mode"])

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def push_to_loki(log_message: str, trace_id: str, level: str = "info"):
    try:
        payload = {
            "streams": [
                {
                    "stream": {
                        "app": "ttc-collector",
                        "level": level,
                        "service_name": "ttc-collector"
                    },
                    "values": [
                        [str(time.time_ns()), f"{log_message} | trace_id={trace_id}"]
                    ]
                }
            ]
        }
        requests.post("http://loki:3100/loki/api/v1/push", json=payload, timeout=1.0)
    except Exception:
        pass

def dispatch_webhooks(event_type: str, payload: dict):
    with tracer.start_as_current_span("dispatch_webhooks") as span:
        webhook_urls = r.smembers("ttc:webhooks")
        span.set_attribute("webhooks.count", len(webhook_urls))
        if not webhook_urls:
            return
            
        for url in webhook_urls:
            try:
                requests.post(url, json={"event": event_type, "timestamp": time.time(), "data": payload}, timeout=2)
                print(f"[Webhook] Dispatched {event_type} to {url}")
            except Exception as e:
                print(f"[Webhook] Failed to dispatch to {url}: {e}")

def _meters(a, b):
    dlat = (a["lat"] - b["lat"]) * 111139
    dlon = (a["lon"] - b["lon"]) * 111139 * math.cos(math.radians(a["lat"]))
    return math.hypot(dlat, dlon)


def mark_bunches(vehicles):
    groups = {}
    for vehicle in vehicles:
        if not vehicle.get("toward") or vehicle.get("speed", 0) < 5:
            continue
        groups.setdefault((vehicle.get("city"), vehicle.get("route"), vehicle.get("toward")), []).append(vehicle)
    for group in groups.values():
        for i, left in enumerate(group):
            for right in group[i + 1:]:
                gap = _meters(left, right)
                if 20 < gap < 400:
                    left["bunched"] = True
                    right["bunched"] = True
    return sum(1 for vehicle in vehicles if vehicle.get("bunched"))


def current_alerts():
    global _ALERTS, _ALERTS_AT
    now = time.time()
    if _ALERTS_AT and now - _ALERTS_AT < 60:
        return _ALERTS
    found = {}
    try:
        response = requests.get(TTC_ALERTS_URL, timeout=8)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            alert = entity.alert
            text = ""
            if alert.header_text.translation:
                text = " ".join(alert.header_text.translation[0].text.split())
            if not text:
                continue
            text = text[:180]
            for informed in alert.informed_entity:
                if informed.HasField("route_id") and informed.route_id not in found:
                    found[informed.route_id] = text
    except Exception as exc:
        print(f"[alerts] {exc}")
        return _ALERTS
    _ALERTS = found
    _ALERTS_AT = now
    return found


def _compass_word(bearing):
    return COMPASS_WORDS[round((bearing % 360) / 45) % 8]


def _pretty_route(route_id):
    text = str(route_id or "").strip()
    if text.endswith("-VIC"):
        text = text[:-4]
    if text.isdigit():
        return str(int(text))
    stripped = text.lstrip("0")
    return stripped or text or "Unknown"


def _parse_agency(content, agency):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    vehicles = []
    alerts = current_alerts() if agency["id"] == "ttc" else {}
    feed_stamp = feed.header.timestamp or time.time()
    feed_age = max(0.0, time.time() - float(feed_stamp))
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.position.HasField("latitude") or not v.position.HasField("longitude"):
            continue
        speed_mps = v.position.speed if v.position.HasField("speed") else 0.0
        speed = round(speed_mps * 3.6, 1)
        has_bearing = v.position.HasField("bearing")
        bearing = round(v.position.bearing, 1) if has_bearing else 0.0
        trip_id = v.trip.trip_id if v.trip.HasField("trip_id") else ""
        raw_id = v.vehicle.id if v.vehicle.HasField("id") else entity.id
        vehicle_data = {
            "id": f"{agency['id']}:{raw_id}",
            "route": _pretty_route(v.trip.route_id if v.trip.HasField("route_id") else ""),
            "lat": round(v.position.latitude, 5),
            "lon": round(v.position.longitude, 5),
            "speed": speed,
            "bearing": bearing,
            "timestamp": int(v.timestamp or 0),
            "city": agency["city"],
            "agency": agency["name"],
        }
        if agency["id"] == "ttc":
            vehicle_data.update(direction_for(trip_id))
            stop_id = v.stop_id if v.HasField("stop_id") else ""
            status_code = v.current_status if v.HasField("current_status") else 2
            place = stop_name(stop_id)
            if place:
                vehicle_data["stop"] = f"{STOP_STATUS.get(status_code, 'Next stop')} {place}"
            if v.HasField("occupancy_status") and v.occupancy_status in OCCUPANCY:
                vehicle_data["occupancy"] = OCCUPANCY[v.occupancy_status]
            notice = alerts.get(vehicle_data["route"])
            if notice:
                vehicle_data["alert"] = notice
        elif agency["id"] == "calgary":
            vehicle_data.update(calgary_trip(trip_id))
            if has_bearing:
                vehicle_data["compass"] = _compass_word(bearing)
        elif has_bearing:
            vehicle_data["compass"] = _compass_word(bearing)
        if not vehicle_data.get("route"):
            vehicle_data["route"] = "Unknown"
        vehicles.append(vehicle_data)
    return vehicles, feed_age, alerts


def fetch_and_store_ttc_vehicles():
    start_time = time.time()
    with tracer.start_as_current_span("ttc_sync_iteration") as root_span:
        try:
            all_vehicles = []
            agency_status = {}
            ttc_age = 0.0
            ttc_alerts = {}
            for agency in AGENCIES:
                try:
                    with tracer.start_as_current_span("fetch_gtfs_rt_feed") as fetch_span:
                        fetch_span.set_attribute("http.url", agency["url"])
                        fetch_span.set_attribute("agency.id", agency["id"])
                        response = requests.get(agency["url"], timeout=12, headers=FEED_HEADERS)
                        response.raise_for_status()
                        fetch_span.set_attribute("http.status_code", response.status_code)
                        fetch_span.set_attribute("payload.size_bytes", len(response.content))
                    with tracer.start_as_current_span("parse_protobuf_payload") as parse_span:
                        vehicles, feed_age, alerts = _parse_agency(response.content, agency)
                        parse_span.set_attribute("protobuf.entities_count", len(vehicles))
                        parse_span.set_attribute("agency.id", agency["id"])
                    HELD[agency["id"]] = vehicles
                    agency_status[agency["city"]] = {
                        "vehicles": len(vehicles),
                        "feed_age_seconds": round(feed_age, 1),
                        "ok": True,
                    }
                    if agency["id"] == "ttc":
                        ttc_age = feed_age
                        ttc_alerts = alerts
                except Exception as exc:
                    SYNC_ERRORS_COUNTER.inc()
                    held = HELD.get(agency["id"], [])
                    print(f"[{time.strftime('%X')}] {agency['city']} feed: {exc}")
                    agency_status[agency["city"]] = {
                        "vehicles": len(held),
                        "feed_age_seconds": None,
                        "ok": False,
                    }
                    vehicles = held
                all_vehicles.extend(vehicles)

            total_vehicles = len(all_vehicles)
            ttc_vehicles = [item for item in all_vehicles if item.get("city") == "Toronto"]
            total_speed = 0.0
            speed_count = 0
            stalled_count = 0
            for item in ttc_vehicles:
                if item.get("speed", 0) > 0:
                    total_speed += item["speed"]
                    speed_count += 1
                else:
                    stalled_count += 1

            with tracer.start_as_current_span("redis_pipeline_cache") as redis_span:
                pipeline = r.pipeline()
                pipeline.delete("ttc:vehicles:next")
                feed_age = ttc_age
                alerts = ttc_alerts

                bunched_count = mark_bunches(all_vehicles)
                ttc_bunched = sum(1 for item in ttc_vehicles if item.get("bunched"))
                full_count = sum(1 for item in ttc_vehicles if item.get("occupancy") in {"Full", "Packed", "Not boarding"})
                for item in all_vehicles:
                    pipeline.hset("ttc:vehicles:next", item["id"], json.dumps(item))

                if total_vehicles:
                    pipeline.rename("ttc:vehicles:next", "ttc:vehicles")
                pipeline.set("ttc:last_updated", time.time())
                pipeline.set("ttc:total_count", len(ttc_vehicles))
                pipeline.set("ttc:avg_speed", round(total_speed / max(speed_count, 1), 1))
                pipeline.set("ttc:moving_vehicles", speed_count)
                pipeline.set("ttc:feed_age", round(feed_age, 1))
                pipeline.execute()
                redis_span.set_attribute("redis.vehicles_cached", total_vehicles)
                redis_span.set_attribute("redis.stalled_vehicles", stalled_count)

            # Prioritize moving vehicles in the broadcast so motion is obvious
            broadcast_vehicles = all_vehicles

            # 4. Publish Event for WebSockets
            with tracer.start_as_current_span("pubsub_broadcast") as pub_span:
                broadcast_payload = {
                    "type": "FLEET_UPDATE",
                    "active_vehicles": len(ttc_vehicles),
                    "moving_vehicles": speed_count,
                    "avg_speed": round(total_speed / max(speed_count, 1), 1),
                    "feed_age_seconds": round(feed_age, 1),
                    "agencies": agency_status,
                    "timestamp": time.time(),
                    "full": True,
                    "trace_id": format(root_span.get_span_context().trace_id, "032x"),
                    "vehicles": broadcast_vehicles
                }
                r.publish("ttc:broadcast", json.dumps(broadcast_payload))
                pub_span.set_attribute("pubsub.channel", "ttc:broadcast")

            # Check for incident trigger and dispatch webhook if needed
            if stalled_count > 100:
                dispatch_webhooks("TRAFFIC_CONGESTION_ALERT", {
                    "stalled_vehicles": stalled_count,
                    "total_vehicles": total_vehicles,
                    "severity": "HIGH"
                })

            # Update Prometheus metrics
            ACTIVE_VEHICLES_GAUGE.labels(route_type="surface").set(len(ttc_vehicles))
            MOVING_VEHICLES_GAUGE.set(speed_count)
            VEHICLES_SYNCED_COUNTER.inc(len(ttc_vehicles))
            if speed_count > 0:
                AVG_SPEED_GAUGE.set(round(total_speed / speed_count, 2))
            FEED_AGE_GAUGE.set(feed_age)
            FULL_VEHICLES_GAUGE.set(full_count)
            BUNCHED_VEHICLES_GAUGE.set(ttc_bunched)
            ALERTS_GAUGE.set(len(alerts))
            root_span.set_attribute("sync.feed_age_seconds", round(feed_age, 1))
            root_span.set_attribute("sync.full_vehicles", full_count)
            root_span.set_attribute("sync.bunched_vehicles", ttc_bunched)
            root_span.set_attribute("sync.alerts", len(alerts))

            ROUTE_VEHICLES_GAUGE.clear()
            ROUTE_SPEED_GAUGE.clear()

            route_counts = {}
            route_speeds = {}

            for v in ttc_vehicles:
                r_id = str(v.get("route", "Unknown"))
                mode = "bus"
                try:
                    r_num = int(r_id)
                    if (501 <= r_num <= 514) or r_num in [301, 304, 306, 310]:
                        mode = "streetcar"
                    elif 900 <= r_num <= 999:
                        mode = "express"
                except (ValueError, TypeError):
                    pass

                key = (r_id, mode)
                route_counts[key] = route_counts.get(key, 0) + 1
                if v.get("speed", 0) > 0:
                    route_speeds.setdefault(key, []).append(v["speed"])

            for (r_id, mode), count in route_counts.items():
                ROUTE_VEHICLES_GAUGE.labels(route=r_id, mode=mode).set(count)
                speeds = route_speeds.get((r_id, mode), [])
                avg_s = round(sum(speeds) / len(speeds), 1) if speeds else 0.0
                ROUTE_SPEED_GAUGE.labels(route=r_id, mode=mode).set(avg_s)
                
            duration = time.time() - start_time
            LAST_SYNC_GAUGE.set(duration)
            trace_id_str = format(root_span.get_span_context().trace_id, "032x")
            if trace_id_str and set(trace_id_str) != {"0"}:
                SYNC_DURATION_HISTOGRAM.observe(duration, exemplar={"trace_id": trace_id_str})
            else:
                SYNC_DURATION_HISTOGRAM.observe(duration)
            root_span.set_attribute("sync.duration_seconds", duration)
            root_span.set_attribute("sync.vehicles_count", total_vehicles)
            city_bits = ", ".join(
                f"{city} {info['vehicles']}" for city, info in agency_status.items()
            )
            sync_log = f"Synced {city_bits} in {duration:.2f}s"
            print(f"[{time.strftime('%X')}] {sync_log} (Trace ID: {trace_id_str[:8]}...)")
            push_to_loki(sync_log, trace_id_str, level="info")
        except Exception as e:
            SYNC_ERRORS_COUNTER.inc()
            root_span.record_exception(e)
            err_trace_id = format(root_span.get_span_context().trace_id, "032x") if root_span else "N/A"
            print(f"[{time.strftime('%X')}] Error fetching TTC feed: {e}")
            push_to_loki(f"Error fetching TTC feed: {e}", err_trace_id, level="error")

if __name__ == "__main__":
    print(f"Starting Prometheus Collector Metrics Server on port {METRICS_PORT}...")
    start_http_server(METRICS_PORT)
    print("Starting TTC GTFS-RT Real-Time Collector loop with Jaeger Tracing...")
    ensure_loaded()
    ensure_calgary()
    while True:
        fetch_and_store_ttc_vehicles()
        time.sleep(10)
