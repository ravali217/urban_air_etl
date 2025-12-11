# extract.py
import requests
import json
import os
import time
import logging
from datetime import datetime

# -----------------------------
# Setup logging
# -----------------------------
logging.basicConfig(
    filename='extract.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# -----------------------------
# Directory to save raw data
# -----------------------------
RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# -----------------------------
# Cities with coordinates
# -----------------------------
CITIES = {
    "Delhi": (28.7041, 77.1025),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Kolkata": (22.5726, 88.3639)
}

# -----------------------------
# API Fetch function with retry
# -----------------------------
def fetch_city_data(city, lat, lon, retries=3, delay=5):
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"
    )
    
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses
            data = response.json()
            return data
        except requests.RequestException as e:
            attempt += 1
            logging.error(f"Error fetching {city} data (Attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    logging.error(f"Failed to fetch data for {city} after {retries} attempts.")
    return None

# -----------------------------
# Save JSON to file
# -----------------------------
def save_json(data, city):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(RAW_DATA_DIR, f"{city}_raw_{timestamp}.json")
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
    logging.info(f"Saved data for {city}: {filename}")
    return filename

# -----------------------------
# Main extraction function
# -----------------------------
def extract_all_cities():
    saved_files = []
    for city, (lat, lon) in CITIES.items():
        logging.info(f"Fetching data for {city} (lat={lat}, lon={lon})")
        data = fetch_city_data(city, lat, lon)
        if data:
            filepath = save_json(data, city)
            saved_files.append(filepath)
        else:
            logging.warning(f"No data saved for {city} due to fetch failure.")
    return saved_files

# -----------------------------
# Run extraction
# -----------------------------
if __name__ == "__main__":
    files = extract_all_cities()
    print("Extraction completed. Saved files:")
    for f in files:
        print(f)
