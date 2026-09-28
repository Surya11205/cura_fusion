import pathway as pw
import pandas as pd
from kafka import KafkaConsumer
import json

# 🔹 Define Schema for Patient Data
class PatientDataSchema(pw.Schema):
    caseid: str
    tname: str
    time: float
    value: float
    age: int
    sex: str
    department: str
    lab_results: dict  # Lab results stored as dictionary

# 🔹 Define Schema for Thresholds
class ThresholdSchema(pw.Schema):
    tname: str
    min_value: float
    max_value: float
    unit: str

# 🔹 Kafka Configuration
rdkafka_settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "pathway_threshold_group",
    "auto.offset.reset": "earliest",
}

# 🔹 Load Thresholds from CSV & Ensure Correct Data Types
thresholds_df = pd.read_csv("thresholds.csv", dtype={"tname": str, "unit": str})
thresholds_df.replace("N/A", pd.NA, inplace=True)

# 🔹 Convert numeric columns, coercing errors & filling missing values
thresholds_df["min_value"] = pd.to_numeric(thresholds_df["min_value"], errors="coerce").fillna(0.0)
thresholds_df["max_value"] = pd.to_numeric(thresholds_df["max_value"], errors="coerce").fillna(0.0)

# 🔹 Ensure "unit" column remains as string
thresholds_df["unit"] = thresholds_df["unit"].astype(str)


# 🔹 Convert Pandas DataFrame to Pathway Table with Correct Schema
thresholds = pw.debug.table_from_pandas(
    df=thresholds_df,
    schema=ThresholdSchema
)

# 🔹 Read Patient Data from Kafka Using Pathway
patient_data = pw.io.kafka.read(
    rdkafka_settings=rdkafka_settings,
    topic="case_tracking",
    schema=PatientDataSchema,
    format="json"
)

# 🔹 Join Patient Data with Thresholds (Real-time)
joined_data = patient_data.join(thresholds, pw.left.tname == pw.right.tname).select(
    *pw.left, pw.right.min_value, pw.right.max_value, pw.right.unit
)

# 🔹 Detect Out-of-Range Values
alerts = joined_data.filter(
    (pw.this.value < pw.this.min_value) | (pw.this.value > pw.this.max_value)
).select(
    pw.this.caseid, 
    pw.this.tname, 
    pw.this.value, 
    pw.this.min_value, 
    pw.this.max_value, 
    pw.this.unit, 
    pw.this.time, 
    pw.this.department
)

# 🔹 Send Alerts to Kafka
pw.io.kafka.write(
    alerts,
    rdkafka_settings=rdkafka_settings,
    topic_name="alerts_topic",
    format="json"
)

# 🔹 Start Pathway Execution
if __name__ == "__main__":
    pw.run() 
