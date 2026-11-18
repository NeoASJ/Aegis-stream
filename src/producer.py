import os
import json
import time
import random
from datetime import datetime, timezone
import requests
import pandas as pd

STREAM_DIR = "data/streaming_inbox"
os.makedirs(STREAM_DIR, exist_ok=True)

CA_BBOX = "-124.48,32.53,-114.13,42.01"

def fetch_weather(lat: float, lon: float) -> dict:
    """Fetches real-time wind speed and direction from Open-Meteo (free, no key)."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&current=temperature_2m,wind_speed_10m,wind_direction_10m"
    )
    try:
        resp = requests.get(url, timeout=5).json()
        current = resp.get("current", {})
        return {
            "temp_c": current.get("temperature_2m", 32.0),
            "wind_speed_kmh": current.get("wind_speed_10m", 18.5),
            "wind_dir_deg": current.get("wind_direction_10m", 225),
        }
    except Exception:
        return {"temp_c": 34.5, "wind_speed_kmh": 24.0, "wind_dir_deg": 240}

def load_fire_events() -> list[dict]:
    """Generates a realistic California wildfire replay stream."""
    print("Initializing California Wildfire Replay Stream...")
    base_clusters = [
        {"zone": "Shasta-Trinity", "lat": 40.7580, "lon": -122.3010},
        {"zone": "Paradise-Hwy70", "lat": 39.7596, "lon": -121.6219},
        {"zone": "LosAngeles-Malibu", "lat": 34.0381, "lon": -118.6923},
    ]

    events = []
    for step in range(50):
        cluster = random.choice(base_clusters)
        drift_lat = (step * 0.0012) + random.uniform(-0.005, 0.005)
        drift_lon = (step * 0.0015) + random.uniform(-0.005, 0.005)
        events.append({
            "latitude": round(cluster["lat"] + drift_lat, 5),
            "longitude": round(cluster["lon"] + drift_lon, 5),
            "bright_ti4": round(random.uniform(330.0, 367.0), 2),
            "frp": round(random.uniform(12.5, 95.0), 2),
            "confidence": random.choice(["nominal", "high"]),
            "satellite": "VIIRS_SNPP",
            "region_hint": cluster["zone"],
        })
    return events

def run_stream():
    events = load_fire_events()
    weather_cache = {}

    print(f"Streaming wildfire files into '{STREAM_DIR}/'... Press Ctrl+C to stop.")
    for idx, event in enumerate(events):
        lat, lon = event["latitude"], event["longitude"]
        coord_key = f"{round(lat, 1)}_{round(lon, 1)}"

        if coord_key not in weather_cache:
            weather_cache[coord_key] = fetch_weather(lat, lon)

        payload = {
            "event_id": f"CA-FIRE-{int(time.time())}-{idx}",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "latitude": lat,
            "longitude": lon,
            "brightness_k": float(event.get("bright_ti4", 340.0)),
            "frp_mw": float(event.get("frp", 25.0)),
            "confidence": str(event.get("confidence", "high")),
            "satellite": str(event.get("satellite", "VIIRS")),
            **weather_cache[coord_key],
        }

        # Write each event as a separate JSON file (Spark reads incoming folder files as streams)
        file_path = os.path.join(STREAM_DIR, f"event_{idx}_{int(time.time())}.json")
        with open(file_path, "w") as f:
            json.dump(payload, f)

        print(f"[{idx+1}/{len(events)}] Wrote file: {os.path.basename(file_path)} | Lat: {lat}, Lon: {lon}")
        time.sleep(1.5)

    print("Producer batch complete.")

if __name__ == "__main__":
    run_stream()
