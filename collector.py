import os
import time
import json
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

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
METRICS_PORT = int(os.getenv("METRICS_PORT", 8001))
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
TTC_FEED_URL = "https://bustime.ttc.ca/gtfsrt/vehicles"

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

def fetch_and_store_ttc_vehicles():
    start_time = time.time()
    with tracer.start_as_current_span("ttc_sync_iteration") as root_span:
        try:
            # 1. Fetch GTFS-RT Feed
            with tracer.start_as_current_span("fetch_gtfs_rt_feed") as fetch_span:
                fetch_span.set_attribute("http.url", TTC_FEED_URL)
                response = requests.get(TTC_FEED_URL, timeout=10)
                response.raise_for_status()
                fetch_span.set_attribute("http.status_code", response.status_code)
                fetch_span.set_attribute("payload.size_bytes", len(response.content))
            
            # 2. Parse Protobuf
            with tracer.start_as_current_span("parse_protobuf_payload") as parse_span:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                total_raw_entities = len(feed.entity)
                parse_span.set_attribute("protobuf.entities_count", total_raw_entities)
            
            total_vehicles = 0
            total_speed = 0.0
            speed_count = 0
            stalled_count = 0
            all_vehicles = []

            # 3. Cache in Redis Pipeline
            with tracer.start_as_current_span("redis_pipeline_cache") as redis_span:
                pipeline = r.pipeline()
                pipeline.delete("ttc:vehicles:next")
                
                for entity in feed.entity:
                    if entity.HasField("vehicle"):
                        v = entity.vehicle
                        # GTFS-RT speed is meters per second. Store km/h so the map and
                        # ttc_fleet_avg_speed_kmh describe the same thing.
                        speed_mps = v.position.speed if v.position.HasField("speed") else 0.0
                        speed = round(speed_mps * 3.6, 1)
                        bearing = round(v.position.bearing, 1) if v.position.HasField("bearing") else 0.0
                        if speed > 0:
                            total_speed += speed
                            speed_count += 1
                        else:
                            stalled_count += 1

                        vehicle_data = {
                            "id": v.vehicle.id if v.vehicle.HasField("id") else entity.id,
                            "route": v.trip.route_id if v.trip.HasField("route_id") else "Unknown",
                            "lat": round(v.position.latitude, 5) if v.position.HasField("latitude") else 0.0,
                            "lon": round(v.position.longitude, 5) if v.position.HasField("longitude") else 0.0,
                            "speed": speed,
                            "bearing": bearing,
                            "timestamp": v.timestamp
                        }
                        pipeline.hset("ttc:vehicles:next", vehicle_data["id"], json.dumps(vehicle_data))
                        total_vehicles += 1
                        all_vehicles.append(vehicle_data)
                        
                if total_vehicles:
                    pipeline.rename("ttc:vehicles:next", "ttc:vehicles")
                pipeline.set("ttc:last_updated", time.time())
                pipeline.set("ttc:total_count", total_vehicles)
                pipeline.set("ttc:avg_speed", round(total_speed / max(speed_count, 1), 1))
                pipeline.set("ttc:moving_vehicles", speed_count)
                pipeline.execute()
                redis_span.set_attribute("redis.vehicles_cached", total_vehicles)
                redis_span.set_attribute("redis.stalled_vehicles", stalled_count)

            # Prioritize moving vehicles in the broadcast so motion is obvious
            broadcast_vehicles = all_vehicles

            # 4. Publish Event for WebSockets
            with tracer.start_as_current_span("pubsub_broadcast") as pub_span:
                broadcast_payload = {
                    "type": "FLEET_UPDATE",
                    "active_vehicles": total_vehicles,
                    "moving_vehicles": speed_count,
                    "avg_speed": round(total_speed / max(speed_count, 1), 1),
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
            ACTIVE_VEHICLES_GAUGE.labels(route_type="surface").set(total_vehicles)
            MOVING_VEHICLES_GAUGE.set(speed_count)
            VEHICLES_SYNCED_COUNTER.inc(total_vehicles)
            if speed_count > 0:
                AVG_SPEED_GAUGE.set(round(total_speed / speed_count, 2))

            ROUTE_VEHICLES_GAUGE.clear()
            ROUTE_SPEED_GAUGE.clear()

            route_counts = {}
            route_speeds = {}

            for v in all_vehicles:
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
            sync_log = f"Synced {total_vehicles} vehicles ({speed_count} moving) in {duration:.2f}s"
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
    while True:
        fetch_and_store_ttc_vehicles()
        time.sleep(10)
