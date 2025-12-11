# transform.py
import os
import json
import pandas as pd
from datetime import datetime

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
STAGED_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "staged"))
os.makedirs(STAGED_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(STAGED_DIR, "air_quality_transformed.csv")

def classify_aqi(pm2_5):
    if pd.isna(pm2_5):
        return None
    pm = float(pm2_5)
    if pm <= 50:
        return "Good"
    elif pm <= 100:
        return "Moderate"
    elif pm <= 200:
        return "Unhealthy"
    elif pm <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def classify_risk(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

def transform_file(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    city = data.get("city") or os.path.basename(path).split("_raw_")[0]
    hourly = data.get("hourly", {})
    if not hourly:
        return pd.DataFrame()
    # hourly expected to have arrays for time and pollutants
    df = pd.DataFrame(hourly)
    df["city"] = city
    # time to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["hour"] = df["time"].dt.hour
    else:
        df["time"] = pd.NaT
        df["hour"] = pd.NA
    # enforce numeric
    poll_cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]
    for c in poll_cols:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    # drop if all pollutants missing
    df.dropna(subset=["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","ozone"], how="all", inplace=True)
    # derived features
    df["severity"] = (
        df["pm2_5"].fillna(0)*5 +
        df["pm10"].fillna(0)*3 +
        df["nitrogen_dioxide"].fillna(0)*4 +
        df["sulphur_dioxide"].fillna(0)*4 +
        df["carbon_monoxide"].fillna(0)*2 +
        df["ozone"].fillna(0)*3
    )
    df["AQI"] = df["pm2_5"].apply(lambda x: classify_aqi(x) if pd.notnull(x) else None)
    df["risk"] = df["severity"].apply(classify_risk)
    # reorder and return
    cols = ["city","time","hour","pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","ozone","uv_index","AQI","severity","risk"]
    return df[cols]

def transform_all():
    print("Looking in:", RAW_DIR)
    files = [os.path.join(RAW_DIR,f) for f in os.listdir(RAW_DIR) if f.lower().endswith(".json")]
    if not files:
        print("No raw JSON files found to transform.")
        return
    all_dfs = []
    for p in files:
        print("Transforming:", os.path.basename(p))
        df = transform_file(p)
        if not df.empty:
            all_dfs.append(df)
    if not all_dfs:
        print("No data after transformation.")
        return
    final = pd.concat(all_dfs, ignore_index=True)
    final.to_csv(OUTPUT_FILE, index=False)
    print("Saved transformed CSV:", OUTPUT_FILE)

if __name__ == "__main__":
    transform_all()
