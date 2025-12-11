# generate_samples.py
import os
import json
from datetime import datetime, timedelta

CITIES = {
    "Delhi": (28.7041, 77.1025),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Kolkata": (22.5726, 88.3639)
}

OUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
os.makedirs(OUT_DIR, exist_ok=True)

def make_hourly_payload():
    # create 24 hourly timestamps and random/sane values
    base = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    times = [(base - timedelta(hours=i)).isoformat() + "Z" for i in reversed(range(24))]
    # create lists of 24 values for each pollutant (simple pattern)
    pm2_5 = [round(40 + (i % 8) * 2 + (i/24), 2) for i in range(24)]
    pm10 = [round(60 + (i % 6) * 3 + (i/24), 2) for i in range(24)]
    co = [round(0.2 + (i % 5) * 0.05, 3) for i in range(24)]
    no2 = [round(15 + (i % 7) * 2, 2) for i in range(24)]
    o3 = [round(10 + (i % 6) * 1.5, 2) for i in range(24)]
    so2 = [round(5 + (i % 4) * 0.8, 2) for i in range(24)]
    uv = [round(max(0, 5 - abs(12 - (i%24))/3), 2) for i in range(24)]
    return {
        "time": times,
        "pm2_5": pm2_5,
        "pm10": pm10,
        "carbon_monoxide": co,
        "nitrogen_dioxide": no2,
        "ozone": o3,
        "sulphur_dioxide": so2,
        "uv_index": uv
    }

def write_samples():
    for city in CITIES.keys():
        payload = {
            "city": city,
            "latitude": CITIES[city][0],
            "longitude": CITIES[city][1],
            "hourly": make_hourly_payload()
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{city}_raw_{ts}.json"
        path = os.path.join(OUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print("Wrote:", path)

if __name__ == "__main__":
    write_samples()
    print("Done. Created sample JSON files in:", OUT_DIR)
