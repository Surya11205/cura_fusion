import streamlit as st
import pandas as pd
import requests
import time
import json
import os
from kafka import KafkaProducer
from patient_agent.user_friendly import fetch_track_list, fetch_track_data,tname_mapping,lab_results_map
# ✅ Constants & Config
KAFKA_BROKER = "localhost:9092"
TRACK_API_BASE_URL = "https://api.vitaldb.net"
REFRESH_INTERVAL = 5  # Auto-refresh interval (in seconds)

# ✅ Load Thresholds
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
thresholds_df = pd.read_csv(os.path.join(BASE_DIR, "thresholds.csv"))

# ✅ Create Threshold Map for Quick Lookup
thresholds_map = {
    row["tname"]: {"min": row["min_value"], "max": row["max_value"], "unit": row["unit"]}
    for _, row in thresholds_df.iterrows()
}

# ✅ Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ✅ Load track name mappings from patient_agent.user_friendly
# This is assumed to be imported or defined locally
# tname_mapping = {
#     "SNUADC/ART": "Arterial Pressure Wave",
#     "SNUADC/CVP": "Central Venous Pressure Wave",
#     "SNUADC/ECG_II": "ECG Lead II Wave",
#     # ... other mappings would be here
# }

# lab_results_map = {
#     "wbc": "White Blood Cell Count",
#     "hb": "Hemoglobin",
#     "hct": "Hematocrit",
#     # ... other mappings would be here
# }

# 🟢 Initialize session state variables
if "monitoring_active" not in st.session_state:
    st.session_state.monitoring_active = False
if "case_id" not in st.session_state:
    st.session_state.case_id = None
if "last_update_time" not in st.session_state:
    st.session_state.last_update_time = time.time()
if "alerts" not in st.session_state:
    st.session_state.alerts = []
if "track_entries" not in st.session_state:
    st.session_state.track_entries = None
if "selected_track" not in st.session_state:
    st.session_state.selected_track = None

# 🎨 Streamlit UI Setup
st.set_page_config(page_title="Patient Monitoring", layout="wide")
st.title("🩺 Real-time Patient Monitoring Dashboard")

# 🧩 Sidebar - Input Controls
st.sidebar.header("⚙️ Monitoring Controls")
case_id_input = st.sidebar.text_input("🔍 Enter Case ID")

# 🚀 Fetch tracks for case ID
def fetch_tracks_callback():
    if case_id_input:
        st.session_state.case_id = case_id_input
        st.session_state.track_entries = fetch_track_list(case_id_input)
        st.session_state.selected_track = None
        st.session_state.monitoring_active = False
        st.session_state.alerts = []
    else:
        st.sidebar.error("❌ Please enter a Case ID")

# 📡 Fetch tracks button
st.sidebar.button("🔍 Fetch Tracks", on_click=fetch_tracks_callback)

# 🔄 Display track selection if tracks are available
if st.session_state.track_entries is not None and not st.session_state.track_entries.empty:
    st.sidebar.success(f"✅ Found {len(st.session_state.track_entries)} tracks for Case ID: {st.session_state.case_id}")
    
    # Create track options for the selectbox with friendly names
    track_options = {}
    for _, track_entry in st.session_state.track_entries.iterrows():
        track_id = track_entry["tid"]
        track_name = track_entry["tname"]
        friendly_name = tname_mapping.get(track_name, f"Unknown ({track_name})")
        track_options[f"{track_id}|{track_name}"] = f"{friendly_name}"
    
    # Track selection dropdown with friendly names
    selected_track_key = st.sidebar.selectbox(
        "📊 Select Track to Monitor",
        options=list(track_options.keys()),
        format_func=lambda x: track_options[x]
    )
    
    if selected_track_key:
        track_id, track_name = selected_track_key.split("|", 1)
        st.session_state.selected_track = {"id": track_id, "name": track_name}

# 🚀 Start/Stop Monitoring Callbacks
def start_monitoring_callback():
    if st.session_state.case_id and st.session_state.selected_track:
        st.session_state.monitoring_active = True
        st.session_state.alerts = []
    else:
        st.sidebar.error("❌ Please select a track to monitor")

def stop_monitoring_callback():
    st.session_state.monitoring_active = False

# 📡 Start/Stop Buttons
if st.session_state.case_id and st.session_state.selected_track:
    if not st.session_state.monitoring_active:
        st.sidebar.button("▶️ Start Monitoring", on_click=start_monitoring_callback)
    else:
        st.sidebar.button("⏹️ Stop Monitoring", on_click=stop_monitoring_callback)
        st.sidebar.success(f"✅ Monitoring Patient ID: {st.session_state.case_id}")
        # Display friendly name if available
        track_name = st.session_state.selected_track['name']
        friendly_name = tname_mapping.get(track_name, track_name)
        st.sidebar.info(f"📡 Tracking: {friendly_name}")

# 🧩 Main Content - Patient Data & Alerts
patient_container = st.container()
alert_container = st.container()

# ✅ Check Thresholds and Generate Alerts
def check_thresholds_and_alert(row):
    caseid = row["caseid"]
    signal_name = row["tname"]
    signal_value = row["value"]
    time_value = row["time"]
    department = row.get("department", "Unknown")

    # Get friendly name for display
    friendly_name = tname_mapping.get(signal_name, signal_name)

    if signal_name in thresholds_map:
        threshold_info = thresholds_map[signal_name]
        min_value, max_value, unit = threshold_info["min"], threshold_info["max"], threshold_info["unit"]

        # 🚨 **Threshold Check**
        if signal_value < min_value or signal_value > max_value:
            alert_message = {
                "caseid": caseid,
                "tname": signal_name,
                "friendly_name": friendly_name,
                "value": signal_value,
                "min_value": min_value,
                "max_value": max_value,
                "time": time_value,
                "department": department,
            }
            # **Send Alert to Kafka**
            producer.send("alerts_topic", alert_message)
            st.warning(
                f"🚨 Alert for {friendly_name}! Value: {signal_value} {unit} \n"
                f"(Range: {min_value}-{max_value})"
            )
            st.session_state.alerts.append(alert_message)
# # 🎯 Chatbot - Query Patient Info (Crew ai one)
# # st.sidebar.header("💬 Patient Query Assistant")
# # user_query = st.sidebar.text_input("🤖 Ask a question about the patient's condition")

# # 🧠 Process Chatbot Query
# def process_chatbot_query():
#     if user_query.strip():
#         patient_agent = PatientAgent()  # ✅ Create PatientAgent instance
#         print("Agent instance created ")
#         response = patient_agent.process_query(user_query)  # ✅ Send query to agent
        
#         # 🎉 Display the response dynamically
#         st.sidebar.success(response)  # ✅ Show real-time response
#     else:
#         st.sidebar.error("❌ Please enter a query to proceed")

# # 📡 Send Chatbot Query Button
# # st.sidebar.button("💬 Send Query", on_click=process_chatbot_query)

# 🔄 Auto-Refresh Mechanism
current_time = time.time()
if st.session_state.monitoring_active:
    if current_time - st.session_state.last_update_time >= REFRESH_INTERVAL:
        with patient_container:
            if st.session_state.selected_track:
                # Fetch data for the selected track
                track_id = st.session_state.selected_track["id"]
                track_name = st.session_state.selected_track["name"]
                track_data = fetch_track_data(track_id, st.session_state.case_id, track_name)
                
                # Update the last update time
                st.session_state.last_update_time = current_time
                
                # Display alerts if any
                if st.session_state.alerts:
                    with alert_container:
                        st.subheader("🚨 Alerts")
                        for alert in st.session_state.alerts:
                            friendly_name = alert.get('friendly_name', alert['tname'])
                            st.warning(
                                f"{friendly_name} - Value: {alert['value']} "
                                f"(Expected: {alert['min_value']} - {alert['max_value']} {alert.get('unit', '')})"
                            )
else:
    st.info("⏸️ Monitoring is paused. Start to receive real-time updates.")