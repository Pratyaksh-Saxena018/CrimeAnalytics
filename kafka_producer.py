import time
import json
import random
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import kagglehub
from kagglehub import KaggleDatasetAdapter

# --- CONFIGURATION ---
KAFKA_TOPIC = "crime_reports"
KAFKA_SERVER = "localhost:9092"  # Change to "127.0.0.1:9092" if connection fails
LOCAL_CSV_PATH = "dataset/crime_dataset_india.csv"
USE_KAGGLE_API = True

# --- SOURCE TYPES (The Fix for KeyError) ---
SOURCE_TYPES = [
    "CCTV_CAM_01", "CCTV_CAM_04", "POLICE_RADIO_DISPATCH",
    "CITIZEN_SOS_APP", "DRONE_SURVEILLANCE", "HELPLINE_CALL_100"
]

# --- CITY GEOLOCATION ---
CITY_COORDS = {
    "Ahmedabad": {"lat": 23.0225, "lon": 72.5714},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Pune": {"lat": 18.5204, "lon": 73.8567},
    "Jaipur": {"lat": 26.9124, "lon": 75.7873},
    "Lucknow": {"lat": 26.8467, "lon": 80.9462},
    "Surat": {"lat": 21.1702, "lon": 72.8311},
    "Kanpur": {"lat": 26.4499, "lon": 80.3319},
    "Nagpur": {"lat": 21.1458, "lon": 79.0882},
    "Indore": {"lat": 22.7196, "lon": 75.8577},
    "Bhopal": {"lat": 23.2599, "lon": 77.4126},
    "Visakhapatnam": {"lat": 17.6868, "lon": 83.2185},
    "Patna": {"lat": 25.5941, "lon": 85.1376},
    "Ludhiana": {"lat": 30.9010, "lon": 75.8573},
    "Agra": {"lat": 27.1767, "lon": 78.0081},
    "Nashik": {"lat": 19.9975, "lon": 73.7898},
    "Ghaziabad": {"lat": 28.6692, "lon": 77.4538},
    "Meerut": {"lat": 28.9845, "lon": 77.7064},
    "Varanasi": {"lat": 25.3176, "lon": 82.9739}
}


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def load_data():
    df = None
    if USE_KAGGLE_API:
        try:
            print("Attempting to load from Kaggle API...")
            df = kagglehub.load_dataset(
                KaggleDatasetAdapter.PANDAS,
                "sudhanvahg/indian-crimes-dataset",
                "crime_dataset_india.csv",
            )
            print("SUCCESS: Loaded data from Kaggle Cloud.")
        except Exception as e:
            print(f"WARNING: Kaggle API failed ({str(e)}). Switching to Local CSV.")

    if df is None:
        try:
            df = pd.read_csv(LOCAL_CSV_PATH, parse_dates=['Date Reported', 'Date of Occurrence'])
            print(f"SUCCESS: Loaded data from {LOCAL_CSV_PATH}")
        except FileNotFoundError:
            print("CRITICAL ERROR: No local CSV found in 'dataset/' folder.")
            return None
    return df


def simulate_stream():
    df = load_data()
    if df is None: return

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=json_serializer
    )
    print(f"--- MULTI-SOURCE STREAM STARTED: Sending to topic '{KAFKA_TOPIC}' ---")

    try:
        while True:
            record = df.sample(1).to_dict(orient="records")[0]

            # --- THE MISSING PART (Fixes KeyError) ---
            record['source_id'] = random.choice(SOURCE_TYPES)

            # Timestamps
            record['original_date'] = str(record['Date Reported'])
            record['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Geolocation + Jitter
            city = record.get('City', 'Unknown')
            center = CITY_COORDS.get(city, {"lat": 20.5937, "lon": 78.9629})

            lat_jitter = random.uniform(-0.05, 0.05)
            lon_jitter = random.uniform(-0.05, 0.05)

            record['latitude'] = center['lat'] + lat_jitter
            record['longitude'] = center['lon'] + lon_jitter

            producer.send(KAFKA_TOPIC, record)

            print(f"[SOURCE: {record['source_id']}] {city} | {record['Crime Description']}")
            time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("\nStopping Stream...")
        producer.close()


if __name__ == "__main__":
    simulate_stream()