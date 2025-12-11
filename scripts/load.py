# load.py
import os
import json
from datetime import datetime

# Optional: Supabase client — only used if credentials supplied
try:
    from supabase import create_client, Client
    HAVE_SUPABASE = True
except Exception:
    HAVE_SUPABASE = False

# Fill these if you want uploads to Supabase.
SUPABASE_URL = "YOUR_SUPABASE_URL"   # e.g. https://xyz.supabase.co
SUPABASE_KEY = "YOUR_SUPABASE_SERVICE_KEY"  # service_role if uploading from backend

USE_SUPABASE = (SUPABASE_URL and SUPABASE_KEY and SUPABASE_URL != "YOUR_SUPABASE_URL" and SUPABASE_KEY != "YOUR_SUPABASE_SERVICE_KEY" and HAVE_SUPABASE)

if USE_SUPABASE:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
    supabase = None

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))

def safe_listdir(path):
    try:
        return os.listdir(path)
    except Exception as e:
        print("Error listing dir:", e)
        return []

def load_all_raw_files():
    print("Loading JSON files into Supabase (if configured)...")
    print("Looking in:", RAW_DIR)
    files = [f for f in safe_listdir(RAW_DIR) if f.lower().endswith(".json")]
    print("Found:", files)
    if not files:
        print("No JSON files found in raw folder.")
        return
    for fname in files:
        path = os.path.join(RAW_DIR, fname)
        print("Processing:", fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print("Failed to read JSON:", e)
            continue
        # Parse timestamp from filename if possible
        city, fetched_time = None, None
        try:
            base = fname.replace(".json","")
            if "_raw_" in base:
                city = base.split("_raw_")[0]
                ts = base.split("_raw_")[1]
                fetched_time = datetime.strptime(ts, "%Y%m%d_%H%M%S")
        except Exception:
            pass
        payload = {
            "filename": fname,
            "city": city or data.get("city"),
            "fetched_at": (fetched_time.isoformat() if fetched_time else None),
            "data": data
        }
        if USE_SUPABASE:
            try:
                resp = supabase.table("air_quality_raw").insert(payload).execute()
                print("Uploaded:", fname)
            except Exception as e:
                print("Upload failed (Supabase):", e)
        else:
            print("Supabase not configured or client not installed — skipping upload.")
    print("Load step complete.")

if __name__ == "__main__":
    load_all_raw_files()
