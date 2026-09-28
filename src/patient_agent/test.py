import pathway as pw
import google.generativeai as genai
import json
import logging
logging.basicConfig(level=logging.DEBUG)

# 🔹 Configure Gemini API
GEMINI_API_KEY = "AIzaSyBIzVjmYvWP0NYG92UkIPwZWD__exss69w"  # Replace with your actual API key
genai.configure(api_key=GEMINI_API_KEY)

# 🔹 Define Patient Data Schema
class PatientDataSchema(pw.Schema):
    caseid: str
    tname: str
    tid: str
    time: float
    value: float
    age: int
    sex: str
    department: str

# 🔹 Define Threshold Schema
class ThresholdSchema(pw.Schema):
    tname: str
    min_value: float
    max_value: float
    unit: str

# 🔹 Kafka Configuration
rdkafka_settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "pathway_group",
    "auto.offset.reset": "earliest",
}

# 🔹 Mapping sensor names (`tname`) to threshold names
tname_mapping = {
    "Solar8000/HR": "Heart Rate",
    "Solar8000/NIBP_SYS": "Blood Pressure Systolic",
    "Solar8000/NIBP_DBP": "Blood Pressure Diastolic",
    "Solar8000/SPO2": "Oxygen Saturation",
    "Solar8000/RR": "Respiratory Rate",
    "Primus/TEMP": "Temperature",
    "Primus/FIN2O": "Blood Glucose Fasting",
    "Primus/FINO2": "Blood Glucose Postprandial",
    "Lab/HGB": "Hemoglobin",
    "Lab/PLT": "Platelet Count",
    "Lab/Na": "Sodium",
    "Lab/K": "Potassium",
    "Lab/Ca": "Calcium",
    "Lab/PH": "Blood pH",
    "Lab/Lactate": "Lactate",
    "Lab/Creat": "Creatinine",
    "Lab/CRP": "C-reactive Protein",
}

# 🔹 Read Data from Kafka
# 🔹 Read Data from Kafka
patient_data = pw.io.kafka.read(
    rdkafka_settings=rdkafka_settings,
    topic="pathway_topic",
    schema=PatientDataSchema,
    format="json",
    autocommit_duration_ms=1000,
)

# 🔹 Print the patient_data to inspect the data
pw.debug.compute_and_print(patient_data)

# 🔹 Add Mapped Column for Threshold Matching
@pw.udf
def map_tname(tname: str) -> str:
    return tname_mapping.get(tname, "Unknown")

patient_data = patient_data.with_columns(
    mapped_tname=map_tname(pw.this.tname)
)
print("Patient data preview:")
print(patient_data.head())

# 🔹 Read Thresholds
thresholds = pw.io.csv.read("thresholds.csv", schema=ThresholdSchema)
pw.debug.compute_and_print(thresholds)    # Check threshold data

# 🔹 Match Patient Data with Thresholds
joined_data = patient_data.join(thresholds, pw.left.mapped_tname == pw.right.tname).select(
    *pw.left, pw.right.min_value, pw.right.max_value, pw.right.unit
)
print(joined_data)
# ✅ Debugging: Check if `joined_data` is correctly formed
# pw.debug.compute_and_print(patient_data)  # Check Kafka data


# 🔹 Detect Abnormal Vitals & Send as Alerts
alerts = joined_data.filter(
    (pw.this.value < pw.this.min_value) | (pw.this.value > pw.this.max_value)
).select(
    pw.this.caseid, pw.this.tname, pw.this.value, pw.this.min_value, pw.this.max_value, pw.this.unit, pw.this.time, pw.this.department
)

# 🔹 Function to Generate LLM-based Alert Messages
def generate_alert_message(caseid, tname, value, min_value, max_value, unit, age, sex, department):
    prompt = f"""
    Patient ID: {caseid}
    Age: {age}, Sex: {sex}, Department: {department}
    Abnormal Vital Detected:
    - **{tname}**: {value} {unit} (Expected: {min_value}-{max_value} {unit})
    
    Generate a medically accurate, professional alert message explaining:
    1. What the abnormal vital means.
    2. Possible medical risks.
    3. Urgency level (Critical, Moderate, Low).
    Format the response as a JSON object with 'message' and 'urgency' fields.
    """
    
    response = genai.GenerativeModel("gemini-pro").generate_content(prompt)

    try:
        alert_data = json.loads(response.text)
        return alert_data.get("message", "No explanation available"), alert_data.get("urgency", "Unknown")
    except:
        return "LLM processing failed.", "Unknown"

# 🔹 Apply LLM Alert Generation
llm_alerts = alerts.with_columns(
    message=pw.udf(generate_alert_message)(
        pw.this.caseid,
        pw.this.tname,
        pw.this.value,
        pw.this.min_value,
        pw.this.max_value,
        pw.this.unit,
        pw.this.age,
        pw.this.sex,
        pw.this.department,
    )
)

# 🔹 Send Processed Alerts to Kafka
pw.io.kafka.write(llm_alerts, rdkafka_settings=rdkafka_settings, topic_name="alerts_topic", format="json")

# # 🔹 Run Pathway Execution
pw.run()
