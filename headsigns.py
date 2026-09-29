"""Join a live TTC trip id to the schedule so a bus can say where it is going."""

import csv
import io
import json
import os
import time
import zipfile
from collections import Counter

import requests

GTFS_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
    "bd4809dd-e289-4de8-bbde-c5c00dafbf4f/resource/"
    "28514055-d011-4ed7-8bb0-97961dfe2b66/download/SurfaceGTFS.zip"
)
CACHE_PATH = os.path.join(os.path.dirname(__file__), "data", "ttc-headsigns.json")
MAX_AGE_SECONDS = 14 * 24 * 3600
COMPASS = {"East", "West", "North", "South"}

CACHE_VERSION = 2

# trip_id -> [route_id, direction_id, compass, toward, short_turn]
TRIPS = {}
# route_id -> {direction_id: opposite terminal name}
ENDS = {}
# stop_id or stop_code -> stop_name
STOPS = {}


def parse_headsign(text):
    raw = (text or "").strip()
    short = "short turn" in raw.lower()
    marker = " towards "
    idx = raw.lower().rfind(marker)
    if idx == -1:
        return "", raw, short
    toward = raw[idx + len(marker):].strip()
    compass = raw[:idx].split("-", 1)[0].strip()
    if compass not in COMPASS:
        compass = ""
    return compass, toward, short


def _build_indexes(trips_file):
    trips = {}
    counts = {}
    reader = csv.DictReader(io.TextIOWrapper(trips_file, encoding="utf-8"))
    for row in reader:
        trip_id = (row.get("trip_id") or "").strip()
        route_id = (row.get("route_id") or "").strip()
        direction = (row.get("direction_id") or "").strip()
        if not trip_id or not route_id:
            continue
        compass, toward, short = parse_headsign(row.get("trip_headsign") or "")
        if not toward:
            continue
        trips[trip_id] = [route_id, direction, compass, toward, 1 if short else 0]
        if not short:
            key = (route_id, direction)
            bucket = counts.setdefault(key, Counter())
            bucket[toward] += 1
    ends = {}
    for (route_id, direction), bucket in counts.items():
        ends.setdefault(route_id, {})[direction] = bucket.most_common(1)[0][0]
    return trips, ends


def _build_stops(stops_file):
    stops = {}
    reader = csv.DictReader(io.TextIOWrapper(stops_file, encoding="utf-8"))
    for row in reader:
        name = (row.get("stop_name") or "").strip()
        if not name:
            continue
        for key in ("stop_id", "stop_code"):
            value = (row.get(key) or "").strip()
            if value:
                stops[value] = name
    return stops


def ensure_loaded():
    if TRIPS and STOPS:
        return
    if _cache_is_current():
        _load_cache()
        return
    try:
        _download_and_cache()
    except Exception as exc:
        print(f"[headsigns] schedule download failed: {exc}")
        if os.path.exists(CACHE_PATH):
            _load_cache()


def _cache_is_current():
    if not os.path.exists(CACHE_PATH):
        return False
    if time.time() - os.path.getmtime(CACHE_PATH) > MAX_AGE_SECONDS:
        return False
    with open(CACHE_PATH, encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload.get("version") == CACHE_VERSION and payload.get("stops") and payload.get("trips")


def _load_cache():
    global TRIPS, ENDS, STOPS
    with open(CACHE_PATH, encoding="utf-8") as handle:
        payload = json.load(handle)
    TRIPS = payload.get("trips") or {}
    ENDS = payload.get("ends") or {}
    STOPS = payload.get("stops") or {}
    print(f"[headsigns] {len(TRIPS)} trips, {len(STOPS)} stops loaded")


def _download_and_cache():
    global TRIPS, ENDS, STOPS
    print("[headsigns] downloading the TTC surface schedule...")
    response = requests.get(GTFS_URL, timeout=180, headers={"User-Agent": "ttc-mesh"})
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        with archive.open("trips.txt") as trips_file:
            trips, ends = _build_indexes(trips_file)
        with archive.open("stops.txt") as stops_file:
            stops = _build_stops(stops_file)
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as handle:
        json.dump(
            {"version": CACHE_VERSION, "trips": trips, "ends": ends, "stops": stops},
            handle,
            separators=(",", ":"),
        )
    TRIPS = trips
    ENDS = ends
    STOPS = stops
    print(f"[headsigns] cached {len(TRIPS)} trips and {len(STOPS)} stops")


def direction_for(trip_id):
    if not trip_id or not TRIPS:
        return {}
    row = TRIPS.get(str(trip_id))
    if not row:
        return {}
    route_id, direction, compass, toward = row[:4]
    short = bool(row[4]) if len(row) > 4 else False
    other_direction = "0" if direction == "1" else "1"
    other = (ENDS.get(route_id) or {}).get(other_direction) or ""
    if other == toward:
        other = ""
    info = {"toward": toward, "compass": compass, "other": other}
    if short:
        info["short_turn"] = True
    return info


def stop_name(stop_id):
    if not stop_id or not STOPS:
        return ""
    return STOPS.get(str(stop_id), "")
