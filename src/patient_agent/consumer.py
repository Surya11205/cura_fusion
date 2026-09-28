from kafka import KafkaConsumer
import json
import time

# 🔹 Kafka Consumers for both case tracking & alerts
case_tracking_consumer = KafkaConsumer(
    "case_tracking",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

alerts_consumer = KafkaConsumer(
    "alerts_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

def display_patient_data(data):
    """ Format and display normal patient case tracking data """
    print("\n📌 **Patient Case Data Received** 📌\n")
    print(f"🔹 **Patient ID**     : {data['caseid']}")
    print(f"🔹 **Track Name**     : {data['tname']}")
    print(f"🔹 **Time Recorded**  : {data['time']} sec")
    print(f"🔹 **Value**          : {data['value']}")
    print(f"🔹 **Age**            : {data['age']} years")
    print(f"🔹 **Sex**            : {data['sex']}")
    print(f"🔹 **Department**     : {data['department']}\n")

    if data.get("lab_results"):
        print("🧪 **Lab Results:**")
        for lab, result in data["lab_results"].items():
            print(f"   - {lab}: {result}")

    print("\n📡 Waiting for new case updates...")
    print("=" * 60)

def display_alert_data(data):
    """ Format and display alerts when a threshold is exceeded """
    print("\n🚨 **ALERT: Threshold Exceeded!** 🚨\n")
    print(f"🔹 **Patient ID**     : {data['caseid']}")
    print(f"🔹 **Track Name**     : {data['tname']}")
    print(f"🔹 **Time Recorded**  : {data['time']} sec")
    print(f"🔹 **Value**          : {data['value']} {data['unit']}")
    print(f"🔹 **Expected Range** : {data['min_value']} - {data['max_value']} {data['unit']}")
    print(f"🔹 **Department**     : {data['department']}\n")

    print("\n📡 Monitoring for new alerts...")
    print("=" * 60)

def get_next_patient():
    """ Function to ask user input for next step """
    while True:
        print("\n🔹 Choose an option:")
        print("1️⃣  Get **real-time patient tracking data**")
        print("2️⃣  Get **alerts for a patient exceeding thresholds**")
        print("3️⃣  Exit")
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == "1":
            return "tracking"
        elif choice == "2":
            return "alerts"
        elif choice == "3":
            print("🚪 Exiting program...")
            exit()
        else:
            print("⚠️ Invalid option! Please enter 1, 2, or 3.")

def fetch_patient_data_by_caseid(case_id):
    """ Fetch specific patient data from `case_tracking` """
    print(f"\n🔍 Fetching data for **Patient ID {case_id}**...\n")
    
    for message in case_tracking_consumer:
        patient_data = message.value
        if patient_data["caseid"] == case_id:
            display_patient_data(patient_data)
            return  # Stop after finding the correct patient data

    print(f"⚠️ No data found for patient ID {case_id}.")

def start_consumer():
    """ Main function to start listening based on user choice """
    print("📡 Listening for case tracking & alerts...\n")

    while True:
        user_choice = get_next_patient()

        if user_choice == "tracking":
            print("\n🎲 Fetching real-time patient tracking data...\n")
            for message in case_tracking_consumer:
                patient_data = message.value
                display_patient_data(patient_data)
                break  # Stop after one record, then ask again

        elif user_choice == "alerts":
            print("\n🚨 Fetching patient alerts...\n")
            for message in alerts_consumer:
                alert_data = message.value
                display_alert_data(alert_data)
                break  # Stop after one record, then ask again

        elif user_choice.isdigit():
            fetch_patient_data_by_caseid(user_choice)

# ✅ Ensure this runs **only when executed directly, not when imported**
if __name__ == "__main__":
    start_consumer()
