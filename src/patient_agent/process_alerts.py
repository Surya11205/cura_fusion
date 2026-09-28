from kafka import KafkaConsumer, KafkaProducer
import streamlit as st
import json
from patient_agent.monitor_patients import thresholds_map
from patient_agent.producer import tname_mapping

# 🔹 Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Update if needed
ALERTS_TOPIC = "alerts_topic"

# 🔹 Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
data_placeholder = st.empty()
def process_alerts_by_id(data):
    """Consume alerts from Kafka and return them as structured data."""
    track = tname_mapping[data['tname']] 
    row = thresholds_map[track] if track in thresholds_map else None
    
    if row:
        min_value = row['min']
        max_value = row['max']
        data_placeholder.empty()
        # Check if value is out of range
        if data['value'] < min_value or data['value'] > max_value:
            # st.write("🚨 Alert: Out-of-range value detected!")
            
            # 🔹 Prepare alert message
            alert_message = {
                "caseid": data["caseid"],
                "tname": data["tname"],
                "value": data["value"],
                "min_value": min_value,
                "max_value": max_value,
                "unit": row.get("unit", "N/A"),
                "time": data["time"],
                "age":data["age"],
                "department": data.get("department", "N/A"),
            }
            
            # 🔹 Send alert to Kafka `alerts_topic`
            producer.send(ALERTS_TOPIC, value=alert_message)
            producer.flush()
            st.write("✅ Alert sent to Kafka!")
            data_placeholder.write(
            f"✅ Alert sent to Kafka!"
        )

        else:
            st.write("✅ Value is within range.")
    else:
        st.write(f"⚠️ No threshold found for track: {track}")
        
        
        
        
# import pathway as pw
# import streamlit as st
# import json
# from patient_agent.monitor_patients import thresholds_map
# from patient_agent.producer import tname_mapping

# # 🔹 Kafka Configuration
# KAFKA_BROKER = "localhost:9092"
# ALERTS_TOPIC = "alerts_topic"
# rdkafka_settings = {
#     "bootstrap.servers": "localhost:9092",
#     "group.id": "pathway_threshold_group",
#     "auto.offset.reset": "earliest",
# }

# # 🔹 Define Alert Schema
# class AlertSchema(pw.Schema):
#     caseid: str
#     tname: str
#     value: float
#     min_value: float
#     max_value: float
#     unit: str
#     time: str
#     department: str

# def process_alerts_by_id(data):
#     """Consume alerts from Kafka and process them using Pathway."""
#     track = tname_mapping[data["tname"]]
#     row = thresholds_map[track] if track in thresholds_map else None

#     if row:
#         min_value = row["min"]
#         max_value = row["max"]

#         if data["value"] < min_value or data["value"] > max_value:
#             st.write("🚨 Alert: Out-of-range value detected!")

#             alert_message = {
#                 "caseid": data["caseid"],
#                 "tname": data["tname"],
#                 "value": data["value"],
#                 "min_value": min_value,
#                 "max_value": max_value,
#                 "unit": row.get("unit", "N/A"),
#                 "time": data["time"],
#                 "department": data.get("department", "N/A"),
#             }

#             # 🔹 Ingest data into Pathway
#             alert_stream = pw.io.python.input(value=[alert_message], schema=AlertSchema)

#             # 🔹 Send alert to Kafka using Pathway
#             pw.io.kafka.write(
#                 alert_stream,
#                 rdkafka_settings=rdkafka_settings,
#                 topic_name="alerts_topic",
#                 format="json"
#             )

#             st.write("✅ Alert sent to Kafka using Pathway!")

#         else:
#             st.write("✅ Value is within range.")
#     else:
#         st.write(f"⚠️ No threshold found for track: {track}")


# # 🔹 Wrapper function for Streamlit
# def run_alert_processing(data):
#     """Wrapper to call `process_alerts_by_id()` safely in Streamlit."""
#     process_alerts_by_id(data)
