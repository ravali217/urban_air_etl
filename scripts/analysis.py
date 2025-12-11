# analysis.py
import os
import pandas as pd

STAGED_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "staged", "air_quality_transformed.csv"))
REPORT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "reports"))
os.makedirs(REPORT_DIR, exist_ok=True)

def analyze_air_quality():
    if not os.path.exists(STAGED_FILE):
        print("Transformed CSV not found. Run transform step first.")
        return
    df = pd.read_csv(STAGED_FILE, parse_dates=["time"], infer_datetime_format=True)
    print("Loaded transformed data. Rows:", len(df))
    # city-wise means
    city_means = df.groupby("city")[["pm2_5","pm10","ozone","nitrogen_dioxide","sulphur_dioxide"]].mean().round(2)
    city_means.to_csv(os.path.join(REPORT_DIR, "city_pollution_summary.csv"))
    print("Saved:", "city_pollution_summary.csv")
    # AQI distribution (AQI column may have nulls)
    if "AQI" in df.columns:
        aqi_counts = df["AQI"].fillna("Unknown").value_counts()
        aqi_counts.to_csv(os.path.join(REPORT_DIR, "aqi_distribution.csv"))
        print("Saved:", "aqi_distribution.csv")
    # risk distribution
    if "risk" in df.columns:
        risk_counts = df["risk"].fillna("Unknown").value_counts()
        risk_counts.to_csv(os.path.join(REPORT_DIR, "risk_distribution.csv"))
        print("Saved:", "risk_distribution.csv")
    # hourly trends
    if "hour" in df.columns:
        hourly = df.groupby("hour")[["pm2_5","pm10"]].mean().round(2)
        hourly.to_csv(os.path.join(REPORT_DIR, "hourly_pollution_trends.csv"))
        print("Saved:", "hourly_pollution_trends.csv")
    print("Analysis complete. Reports in:", REPORT_DIR)

if __name__ == "__main__":
    analyze_air_quality()
