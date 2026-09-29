"""Calgary's live feed has a trip id and no route. The schedule fills that in."""

import csv
import io
import json
import os
import time
import zipfile

import requests

GTFS_URL = "https://data.calgary.ca/download/npk7-z3bj/application%2Fzip"
CACHE_PATH = os.path.join(os.path.dirname(__file__), "data", "calgary-trips.json")
MAX_AGE_SECONDS = 14 * 24 * 3600
CACHE_VERSION = 1

# trip_id -> [route_short_name, headsign, route_type]
TRIPS = {}


def ensure_loaded():
    if TRIPS:
        return
    if _cache_is_current():
        _load_cache()
        return
    try:
        _download_and_cache()
    except Exception as exc:
        print(f"[calgary] schedule download failed: {exc}")
        if os.path.exists(CACHE_PATH):
            _load_cache()


def trip(trip_id):
    if not trip_id or not TRIPS:
        return {}
    row = TRIPS.get(str(trip_id))
    if not row:
        return {}
    short, headsign, route_type = row[0], row[1], row[2] if len(row) > 2 else ""
    info = {}
    if short:
        info["route"] = short
    if headsign:
        info["toward"] = headsign.title() if headsign.isupper() else headsign
    if str(route_type) == "0":
        info["mode"] = "streetcar"
    return info


def _cache_is_current():
    if not os.path.exists(CACHE_PATH):
        return False
    if time.time() - os.path.getmtime(CACHE_PATH) > MAX_AGE_SECONDS:
        return False
    with open(CACHE_PATH, encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload.get("version") == CACHE_VERSION and payload.get("trips")


def _load_cache():
    global TRIPS
    with open(CACHE_PATH, encoding="utf-8") as handle:
        payload = json.load(handle)
    TRIPS = payload.get("trips") or {}
    print(f"[calgary] {len(TRIPS)} trips loaded")


def _download_and_cache():
    global TRIPS
    print("[calgary] downloading the Calgary schedule...")
    response = requests.get(GTFS_URL, timeout=180, headers={"User-Agent": "ttc-mesh"})
    response.raise_for_status()
    routes = {}
    trips = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        with archive.open("routes.txt") as handle:
            for row in csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig")):
                routes[row.get("route_id") or ""] = (
                    (row.get("route_short_name") or "").strip(),
                    (row.get("route_type") or "").strip(),
                )
        with archive.open("trips.txt") as handle:
            for row in csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig")):
                trip_id = (row.get("trip_id") or "").strip()
                route_id = (row.get("route_id") or "").strip()
                if not trip_id or route_id not in routes:
                    continue
                short, route_type = routes[route_id]
                headsign = " ".join((row.get("trip_headsign") or "").split())
                trips[trip_id] = [short or route_id, headsign, route_type]
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as handle:
        json.dump({"version": CACHE_VERSION, "trips": trips}, handle, separators=(",", ":"))
    TRIPS = trips
    print(f"[calgary] cached {len(TRIPS)} trips")
