# import json
# import folium
# from folium.plugins import MarkerCluster, HeatMap
# from kafka import KafkaConsumer
# import os
# import time
# from threading import Thread
#
# # --- CONFIGURATION ---
# KAFKA_TOPIC = "crime_reports"
# KAFKA_SERVER = "localhost:9092"
# MAP_FILE = "live_crime_map.html"
#
# # Buffer to store last 100 crimes for the map
# crime_buffer = []
#
#
# def get_icon(source_id, crime_type):
#     """
#     Returns a distinct icon based on the source.
#     This solves the 'Plain Visualization' problem.
#     """
#     if "CCTV" in source_id:
#         return folium.Icon(color="blue", icon="camera", prefix="fa")
#     elif "POLICE" in source_id:
#         return folium.Icon(color="darkblue", icon="shield", prefix="fa")
#     elif "DRONE" in source_id:
#         return folium.Icon(color="green", icon="plane", prefix="fa")
#     elif "SOS" in source_id:
#         return folium.Icon(color="red", icon="exclamation-triangle", prefix="fa")
#     else:
#         return folium.Icon(color="gray", icon="info-sign")
#
#
# def update_map():
#     """
#     Re-draws the map HTML file every 5 seconds using the buffer data.
#     """
#     while True:
#         if len(crime_buffer) > 0:
#             # 1. Base Map (Dark Theme looks more 'Cyber')
#             m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles="CartoDB dark_matter")
#
#             # 2. Add Heatmap Layer (For density visualization)
#             heat_data = [[row['latitude'], row['longitude']] for row in crime_buffer]
#             HeatMap(heat_data, radius=15).add_to(m)
#
#             # 3. Add Individual Markers (With Multi-Source Icons)
#             marker_cluster = MarkerCluster().add_to(m)
#
#             for row in crime_buffer:
#                 # Create detailed popup content
#                 popup_html = f"""
#                 <b>CRIME ALERT</b><br>
#                 Type: {row['Crime Description']}<br>
#                 Source: {row['source_id']}<br>
#                 Time: {row['timestamp']}<br>
#                 Weapon: {row['Weapon Used']}
#                 """
#
#                 folium.Marker(
#                     location=[row['latitude'], row['longitude']],
#                     popup=folium.Popup(popup_html, max_width=300),
#                     icon=get_icon(row['source_id'], row['Crime Description'])
#                 ).add_to(marker_cluster)
#
#             # 4. Save Map
#             m.save(MAP_FILE)
#             print(f"--- MAP UPDATED ({len(crime_buffer)} incidents) ---")
#
#         time.sleep(5)
#
#
# def consume_stream():
#     """
#     Listens to Kafka and updates the memory buffer.
#     """
#     consumer = KafkaConsumer(
#         KAFKA_TOPIC,
#         bootstrap_servers=[KAFKA_SERVER],
#         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#         auto_offset_reset='latest'
#     )
#
#     print("--- DASHBOARD ENGINE STARTED ---")
#     for message in consumer:
#         data = message.value
#
#         # Keep only last 100 incidents to keep map fast
#         crime_buffer.append(data)
#         if len(crime_buffer) > 100:
#             crime_buffer.pop(0)
#
#
# if __name__ == "__main__":
#     # Run Map Updater in a background thread
#     t = Thread(target=update_map)
#     t.daemon = True
#     t.start()
#
#     # Run Consumer in main thread
#     consume_stream()


import json
import folium
from folium.plugins import MarkerCluster, HeatMap
from kafka import KafkaConsumer
import os
import time
from threading import Thread
import pandas as pd
#it has been changed to add features
# --- CONFIGURATION ---
KAFKA_TOPIC = "crime_reports"
KAFKA_SERVER = "localhost:9092"
MAP_FILE = "live_crime_map.html"

# Buffer to store last 100 crimes for the map
crime_buffer = []


def get_icon(source_id, crime_type):
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


def generate_header_html(total, critical, sources):
    """Generates the 'Krawl Dashboard' style analytical header"""
    return f"""
    <div style="position: fixed; top: 10px; left: 50px; width: 90%; height: 80px; 
                background: rgba(16, 20, 30, 0.9); z-index: 9999; border-radius: 10px; 
                border: 1px solid #333; color: white; padding: 10px; display: flex; 
                justify-content: space-around; align-items: center; font-family: 'Segoe UI', sans-serif;">
        <div style="text-align: center;">
            <div style="font-size: 24px; color: #3498db; font-weight: bold;">{total}</div>
            <div style="font-size: 12px; text-transform: uppercase;">Total Incidents</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: 24px; color: #e74c3c; font-weight: bold;">{critical}</div>
            <div style="font-size: 12px; text-transform: uppercase;">Critical Alerts</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: 24px; color: #2ecc71; font-weight: bold;">{sources}</div>
            <div style="font-size: 12px; text-transform: uppercase;">Unique Sources</div>
        </div>
        <div style="text-align: center; border-left: 1px solid #444; padding-left: 20px;">
            <div style="font-size: 18px; color: #f1c40f;">LIVE MONITORING</div>
            <div style="font-size: 10px;">PROACTIVE POLICING DASHBOARD</div>
        </div>
    </div>
    """


def update_map():
    while True:
        if len(crime_buffer) > 0:
            df = pd.DataFrame(crime_buffer)

            # 1. Base Map (Dark Theme)
            m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles="CartoDB dark_matter")

            # 2. Analytics Calculation
            total_crimes = len(df)
            critical_crimes = len(df[df['Weapon Used'].str.contains('Firearm|Knife|Explosive', na=False, case=False)])
            unique_sources = df['source_id'].nunique()

            # 3. Add Header
            header_html = generate_header_html(total_crimes, critical_crimes, unique_sources)
            m.get_root().html.add_child(folium.Element(header_html))

            # 4. Add Heatmap
            heat_data = [[row['latitude'], row['longitude']] for row in crime_buffer]
            HeatMap(heat_data, radius=15).add_to(m)

            # 5. Marker Cluster with City Stats
            marker_cluster = MarkerCluster().add_to(m)

            # Group by city for "City Stats"
            city_stats = df.groupby('City').size().to_dict()

            for row in crime_buffer:
                city_total = city_stats.get(row['City'], 0)

                # Enhanced Popup with City Statistics
                popup_html = f"""
                <div style="font-family: 'Segoe UI'; width: 200px;">
                    <b style="color: #e74c3c;">CITY ALERT: {row['City']}</b><br>
                    <hr style="margin: 5px 0;">
                    <b>Incident:</b> {row['Crime Description']}<br>
                    <b>Weapon:</b> {row['Weapon Used']}<br>
                    <b>Source:</b> {row['source_id']}<br>
                    <hr style="margin: 5px 0;">
                    <b>City Crime Count:</b> {city_total} (Real-time)<br>
                    <small>Time: {row['timestamp']}</small>
                </div>
                """

                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"City: {row['City']} | Crime: {row['Crime Description']}",  # Added Hover Tooltip
                    icon=get_icon(row['source_id'], row['Crime Description'])
                ).add_to(marker_cluster)

            m.save(MAP_FILE)
            print(f"--- MAP UPDATED (Header & City Stats Active) ---")

        time.sleep(5)


def consume_stream():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print("--- DASHBOARD ENGINE STARTED ---")
    for message in consumer:
        data = message.value
        crime_buffer.append(data)
        if len(crime_buffer) > 100:
            crime_buffer.pop(0)


if __name__ == "__main__":
    t = Thread(target=update_map)
    t.daemon = True
    t.start()
    consume_stream()