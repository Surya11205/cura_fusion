from datetime import timedelta
import json
import os
from dotenv import load_dotenv
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import Tool
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.outputs import ChatResult, ChatGeneration
from typing import Any
import pathway as pw
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import time

# ------------------- Load Environment Variables -------------------
load_dotenv()

# ------------------- Gemini API Configuration -------------------
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY not found. Check your .env file!")
genai.configure(api_key=api_key)

# ------------------- Custom Gemini LLM Wrapper -------------------
class GeminiChatModel(BaseChatModel):
    def _generate(self, messages, stop=None, **kwargs):
        """Generate response using Gemini model."""
        combined_message = "\n".join(
            f"{msg.type.capitalize()}: {msg.content}" if hasattr(msg, "content") else "" for msg in messages
        )
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(combined_message)
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(content=response.text),
                    text=response.text,
                )
            ]
        )
    def bind_tools(self, tools):
        return self
    def _llm_type(self):
        return "gemini"

# ✅ Initialize Gemini LLM
llm = GeminiChatModel()

# ------------------- Kafka Configuration -------------------
rdkafka_settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "pathway_group",
    "auto.offset.reset": "latest",
}

# Define the schema for incoming Kafka messages
class AlertSchema(pw.Schema):
    caseid: str
    tname: str
    value: float
    min_value: float
    max_value: float
    unit: str
    time: float
    department: str

# Read messages from the Kafka topic
alerts_stream = pw.io.kafka.read(
    rdkafka_settings=rdkafka_settings,
    topic="alerts_topic",
    schema=AlertSchema,
    format="json",
)

# Optional: Print the raw schema for debugging
print("Raw alerts schema:", alerts_stream.schema)

# ------------------- Deduplicate Alerts -------------------
# Here we use a tumbling window of 60 seconds and group by caseid,
# taking only the first alert within each window for each case.
deduped_alerts = alerts_stream.windowby(
    alerts_stream.time,
    window=pw.temporal.tumbling(duration=60),
    instance=alerts_stream.caseid
).reduce(
    caseid     = pw.reducers.earliest(pw.this.caseid),
    tname      = pw.reducers.earliest(pw.this.tname),
    value      = pw.reducers.earliest(pw.this.value),
    min_value  = pw.reducers.earliest(pw.this.min_value),
    max_value  = pw.reducers.earliest(pw.this.max_value),
    unit       = pw.reducers.earliest(pw.this.unit),
    time       = pw.reducers.earliest(pw.this.time),
    department = pw.reducers.earliest(pw.this.department)
)




# ------------------- Define Function to Print and Query Gemini -------------------
# This function prints the alert and sends it to Gemini.
def print_message(caseid: str, tname: str, value: float, min_value: float, max_value: float,
                  unit: str, time_val: float, department: str) -> str:
    message = (
        f"Received alert - Case ID: {caseid}, Parameter: {tname}, Value: {value} {unit}, "
        f"Expected Range: {min_value}-{max_value} {unit}, Time: {time_val}, Department: {department}"
    )
    print(message)
    # Prepare a prompt for Gemini (if needed)
    prompt = f"""
    You are a medical assistant. Analyze the following alert:
    {message}
    
    Provide a concise summary and recommendations.
    """
    response = llm.invoke([HumanMessage(content=prompt)])
    # Print Gemini's response
    print(f"🤖 Gemini response for Case {caseid}: {response}")
    # Sleep for a short period to avoid rapid-fire API calls (adjust as needed)
    time.sleep(5)
    # Return the original message (or any placeholder) so that pw.apply produces a column of type str
    return message

# ------------------- Apply the Function to Deduplicated Alerts -------------------
final_alerts = deduped_alerts.select(
    debug_output = pw.apply(
        print_message,
        deduped_alerts.caseid,
        deduped_alerts.tname,
        deduped_alerts.value,
        deduped_alerts.min_value,
        deduped_alerts.max_value,
        deduped_alerts.unit,
        deduped_alerts.time,
        deduped_alerts.department
    )
)

# Materialize the table and print the results (for debugging)
pw.debug.compute_and_print(final_alerts)

# ------------------- Run the Pathway Computation -------------------
# Use run_forever to continuously monitor alerts.
pw.run()
