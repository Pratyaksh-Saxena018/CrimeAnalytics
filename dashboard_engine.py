import json
import folium
from folium.plugins import MarkerCluster, HeatMap
from kafka import KafkaConsumer
import os
import time
from threading import Thread

# --- CONFIGURATION ---
KAFKA_TOPIC = "crime_reports"
KAFKA_SERVER = "localhost:9092"
MAP_FILE = "live_crime_map.html"

# Buffer to store last 100 crimes for the map
crime_buffer = []


def get_icon(source_id, crime_type):
    """
    Returns a distinct icon based on the source.
    This solves the 'Plain Visualization' problem.
    """
    if "CCTV" in source_id:
        return folium.Icon(color="blue", icon="camera", prefix="fa")
    elif "POLICE" in source_id:
        return folium.Icon(color="darkblue", icon="shield", prefix="fa")
    elif "DRONE" in source_id:
        return folium.Icon(color="green", icon="plane", prefix="fa")
    elif "SOS" in source_id:
        return folium.Icon(color="red", icon="exclamation-triangle", prefix="fa")
    else:
        return folium.Icon(color="gray", icon="info-sign")


def update_map():
    """
    Re-draws the map HTML file every 5 seconds using the buffer data.
    """
    while True:
        if len(crime_buffer) > 0:
            # 1. Base Map (Dark Theme looks more 'Cyber')
            m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles="CartoDB dark_matter")

            # 2. Add Heatmap Layer (For density visualization)
            heat_data = [[row['latitude'], row['longitude']] for row in crime_buffer]
            HeatMap(heat_data, radius=15).add_to(m)

            # 3. Add Individual Markers (With Multi-Source Icons)
            marker_cluster = MarkerCluster().add_to(m)

            for row in crime_buffer:
                # Create detailed popup content
                popup_html = f"""
                <b>CRIME ALERT</b><br>
                Type: {row['Crime Description']}<br>
                Source: {row['source_id']}<br>
                Time: {row['timestamp']}<br>
                Weapon: {row['Weapon Used']}
                """

                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=get_icon(row['source_id'], row['Crime Description'])
                ).add_to(marker_cluster)

            # 4. Save Map
            m.save(MAP_FILE)
            print(f"--- MAP UPDATED ({len(crime_buffer)} incidents) ---")

        time.sleep(5)


def consume_stream():
    """
    Listens to Kafka and updates the memory buffer.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )

    print("--- DASHBOARD ENGINE STARTED ---")
    for message in consumer:
        data = message.value

        # Keep only last 100 incidents to keep map fast
        crime_buffer.append(data)
        if len(crime_buffer) > 100:
            crime_buffer.pop(0)


if __name__ == "__main__":
    # Run Map Updater in a background thread
    t = Thread(target=update_map)
    t.daemon = True
    t.start()

    # Run Consumer in main thread
    consume_stream()