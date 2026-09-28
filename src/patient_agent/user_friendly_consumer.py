import io
from kafka import KafkaConsumer,KafkaProducer
import json
import time
from patient_agent.user_friendly import tname_mapping,lab_results_map  # ✅ Import function to fetch missing data
import requests
# 🔹 Kafka Consumer Setup
consumer = KafkaConsumer(
    "case_tracking",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=10000
)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# 🔹 Load track list & cases data
import os
import pandas as pd

# Get the current script directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Construct absolute paths
track_list_path = os.path.join(BASE_DIR, "track_list.xlsx")
cases_df_path = os.path.join(BASE_DIR, "cases.xlsx")
labs_df_path = os.path.join(BASE_DIR, "vitaldb_labs.xlsx")

# Read Excel files
track_list = pd.read_excel(track_list_path)
cases_df = pd.read_excel(cases_df_path)
labs_df = pd.read_excel(labs_df_path)

def display_patient_data(data):
    """ Format and display patient case tracking data """
    print("\n📌 **Patient Case Data Received** 📌\n")
    print(f"🔹 **Patient ID**     : {data['caseid']}")
    print(f"🔹 **Track Name**     : {data['tname']}")
    print(f"🔹 **Time Recorded**  : {data['time']} sec")
    print(f"🔹 **Value**          : {data['value']}")
    print(f"🔹 **Age**            : {data['age']} years")
    print(f"🔹 **Sex**            : {data['sex']}")
    print(f"🔹 **Department**     : {data['department']}\n")

    if "lab_results" in data:
        print("🧪 **Lab Results:**")
        for lab, result in data["lab_results"].items():
            print(f"   - {lab}: {result}")

    print("\n📡 Waiting for new case updates...")
    print("=" * 60)

def get_next_patient():
    """ Function to ask user input for next step """
    while True:
        print("\n🔹 Choose an option:")
        print("1️⃣  Get data for a **random patient**")
        print("2️⃣  Get data for a **specific patient** (enter case ID)")
        print("3️⃣  Exit")
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == "1":
            return "random"
        elif choice == "2":
            case_id = input("Enter the **Case ID**: ").strip()
            if case_id.isdigit():
                return case_id
            else:
                print("⚠️ Invalid Case ID. Please enter a number.")
        elif choice == "3":
            print("🚪 Exiting program...")
            exit()
        else:
            print("⚠️ Invalid option! Please enter 1, 2, or 3.")

def fetch_track_data(caseid,tname):
    """Fetch all tracks for a given Case ID and send to Kafka."""
    tname = next((key for key, value in tname_mapping.items() if value == tname), None)

    # **Get All Track IDs for Given Case ID**
    print(f"🔍 Searching tracks for Case ID: {caseid}")
    track_entries = track_list[(track_list["caseid"] == int(caseid)) & (track_list["tname"] == tname)]
    print(" tracks found = ", len(track_entries))
    time.sleep(3)
    if track_entries.empty:
        print(f"❌ No tracks found for Case ID {caseid}")
        return

    # **Iterate Over All Matching Tracks**
    for _, track_entry in track_entries.iterrows():
        track_id = track_entry["tid"]
        print(f"Processing Track ID: {track_id}")
        time.sleep(3)

        # **🔹 API Fetch with Retry Logic**
        max_retries, retry_delay = 3, 5
        track_data = None

        for attempt in range(max_retries):
            response = requests.get(f"https://api.vitaldb.net/{track_id}", timeout=10)
            if response.status_code == 200 and response.content:
                track_data = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                break
            else:
                print(f"⚠️ Retry {attempt + 1}/{max_retries} for {track_id} failed. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

        if track_data is None or track_data.empty:
            print(f"❌ No valid data found for {track_id}. Skipping...")
            continue
        
        print(f"✅ Fetched Data for {track_id} - {track_data.shape[0]} rows")

        # **Filter Invalid/Missing Values**
        track_data = track_data.dropna()
        track_data = track_data[track_data.iloc[:, 0] > 0]  # Remove time=0 rows
        track_data = track_data[track_data.iloc[:, 1] > 0]  # Remove value=0 rows

        if track_data.empty:
            print(f"⚠️ No valid rows after filtering for {track_id}. Skipping...")
            continue

        # **Fetch Patient Details**
        patient_info = cases_df[cases_df["caseid"] == int(caseid)].to_dict(orient="records")
        patient_details = patient_info[0] if patient_info else {}

        # **Fetch Lab Results**
        lab_results = labs_df[labs_df["caseid"] == int(caseid)].to_dict(orient="records")
        mapped_lab_results = {lab_results_map.get(lab["name"], lab["name"]): lab["result"] for lab in lab_results}

        # **Send All Rows to Kafka**
        for _, row in track_data.iterrows():
            time_value = float(row.iloc[0]) if not pd.isna(row.iloc[0]) else 0.0

            for signal_name in track_data.columns[1:]:
                signal_value = float(row[signal_name]) if not pd.isna(row[signal_name]) else 0.0

                # **Map signal name to user-friendly name**
                mapped_signal_name = tname_mapping.get(signal_name, signal_name)

                # **Prepare Kafka Message**
                message = {
                    "caseid": caseid,
                    "tname": mapped_signal_name,
                    "tid": track_id,
                    "time": time_value,
                    "value": signal_value,
                    "age": int(patient_details.get("age", 0)) if not pd.isna(patient_details.get("age", 0)) else 0,
                    "sex": str(patient_details.get("sex", "Unknown")),
                    "department": str(patient_details.get("department", "Unknown")),
                    "lab_results": mapped_lab_results,
                }

                # **Send Message to Kafka**
                producer.send("case_tracking", message)
                print("message = ",message)
                time.sleep(5)

        print(f"📤 Sent All Data for Track ID {track_id} to Kafka\n")
        time.sleep(3)


def fetch_patient_data_by_caseid(case_id):
    """Fetch and display data for all tracks of a specific Case ID."""
    print(f"\n🔍 Searching for data on **Case ID {case_id}**...\n")

    found_data = []

    # **Loop Until All Tracks for the Case ID are Processed**
    start_time = time.time()
    timeout = 10  # 10 seconds to wait for messages

    while time.time() - start_time < timeout:
        for message in consumer:
            patient_data = message.value
            if patient_data["caseid"] == case_id:
                found_data.append(patient_data)

        if found_data:
            break

    if found_data:
        # **Display Data for All Tracks**
        for data in found_data:
            return data
    else:
        print(f"⚠️ No data found for patient ID {case_id}. Fetching from API...\n")
        fetch_track_data(case_id)  # ✅ Fetch all tracks and send to Kafka
        print(f"🔄 **Waiting for Kafka to receive the new data...**")
        time.sleep(5)  # ✅ Allow time for Kafka to process
        return fetch_patient_data_by_caseid(case_id)




# 🔹 Start Listening
if __name__=="main":
    print("📡 Listening for case tracking updates...\n")

    while True:
        user_choice = get_next_patient()

        if user_choice == "random":
            print("\n🎲 Fetching random patient data...\n")
            fetch_track_data("random")  # ✅ Modify if you have a random fetch logic
        else:
            fetch_patient_data_by_caseid(user_choice)
