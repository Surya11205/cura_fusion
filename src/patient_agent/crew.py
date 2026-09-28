from crewai import Agent, Crew, Task
from crewai.tools import tool
# from patient_agent.monitor_patients import monitor_all_patients
from patient_agent.user_friendly_consumer import fetch_track_data
from patient_agent.process_alerts import process_alerts_by_id
from typing import Any
import time
from patient_agent.tools.query_parser import QueryParser
# ✅ Define a tool for fetching patient data
@tool("Fetch Patient Data")
def fetch_patient_data(patient_id: int,parameter:str) -> str:
    """Fetches real-time patient case data from the system."""
    print(f"📡 Fetching data for Patient ID: {patient_id}")
    patient_data = fetch_track_data(patient_id,parameter)
    print(f"📊 Patient data received: {patient_data}")
    return patient_data


# ✅ Define a tool for processing alerts
# @tool("Process Alerts")
# def get_alerts(patient_id: int) -> str:
#     """Fetches alerts related to the patient's case for later analysis."""
#     print(f"🚨 Fetching alerts for Patient ID: {patient_id}")
#     alerts = process_alerts_by_id(patient_id)
#     print(f"⚡️ Alerts received: {alerts}")
#     return alerts


# ✅ Define a tool for monitoring all patients
# @tool("Monitor All Patients")
# def monitor_all_patients_tool() -> str:
#     """Monitors all patient vitals and flags threshold breaches."""
#     print("🩺 Monitoring all patients for threshold breaches...")
#     result = monitor_all_patients()
#     print(f"✅ Monitoring result: {result}")
#     return result


# ✅ Define PatientAgent Class
from multiprocessing import Manager
import threading


class PatientAgent:
    def __init__(self):
        self.query_parser = QueryParser()
        # ✅ Initialize shared state
        manager = Manager()
        self.shared_data = manager.dict()

        # ✅ Define monitoring agent
        # self.monitoring_agent = Agent(
        #     name="Threshold Monitoring Agent",
        #     role=(
        #         "Monitor patient vitals and check for abnormal threshold values. "
        #         "This tool accepts only full numbers as patient IDs. DO NOT provide unnecessary input except the exact user-given ID."
        #     ),
        #     goal="Generate alerts when any patient’s vitals exceed safe limits.",
        #     backstory="This agent continuously monitors patient vitals and flags threshold breaches.",
        #     expected_output="A detailed report listing patients whose vitals exceeded safe limits, with alerts if any.",
        #     tools=[fetch_patient_data, get_alerts,],
        #     verbose=True,
        # )

        # ✅ Define chatbot agent
        self.chatbot_agent = Agent(
            name="User Interaction Agent",
            role=(
                "Handle real-time patient queries and provide status updates. "
                "Use up-to-date data to ensure doctors receive the latest information."
            ),
            goal="Provide real-time updates on patient status, including any critical alerts.",
            backstory="This agent ensures that doctors receive live updates with actionable insights.",
            expected_output="A concise and actionable response about the patient's condition.",
            human_input=True
        )

        # ✅ Define data fetch agent for background task
        self.data_fetch_agent = Agent(
            name="Patient Data Retrieval Specialist",
            role="Fetch and update real-time patient data in the background continuously.",
            goal="Ensure that patient data remains current and is available for downstream agents.",
            backstory=" This agent specializes in collecting, analyzing, and preparing real-time patient data for efficient decision-making.",
            expected_output="Continuous updates to patient data, keeping it available for other agents.",
            human_input=True,
            tools=[fetch_patient_data,]
        )

        # ✅ Define continuous monitoring task
        # self.monitor_all_task = Task(
        #     description="Monitor all patients and flag any threshold breaches.",
        #     agent=self.monitoring_agent,
        #     expected_output="A complete analysis of all patients' vitals, highlighting any threshold breaches and corresponding alerts.",
        # )

        # # ✅ Define individual monitoring task dynamically for patient ID
        # self.monitoring_task = Task(
        #     description="Analyze patient data and detect any threshold breaches. Report alerts if the vitals exceed safe limits.",
        #     agent=self.monitoring_agent,
        #     expected_output="A comprehensive analysis of the patient's vitals, listing all anomalies and triggered alerts.",
        # )

        # ✅ Define chatbot task
        self.chatbot_task = Task(
            description="Respond to user inquiries regarding patient status and provide real-time information.",
            agent=self.chatbot_agent,
            expected_output="A summary of the patient's status, including vital data and alert information if applicable.",
        )

        # ✅ Define data fetch task to run in background
        self.data_fetch_task = Task(
            description="Continuously fetch patient data and update shared state for downstream agents.",
            agent=self.data_fetch_agent,
            expected_output="Updated patient data stored in shared state.",
        )

        # ✅ Create Crew with Agents and Tasks
        self.crew_instance = Crew(
            agents=[
                # self.monitoring_agent,
                self.chatbot_agent,
                self.data_fetch_agent,
            ],
            tasks=[
                # self.monitor_all_task,
                # self.monitoring_task,
                self.chatbot_task,
                self.data_fetch_task,
            ],
            verbose=True,
        )

        # ✅ Start background agents
        # self.start_background_agents()
    def crew(self):
        """Return the crew instance for execution."""
        return self.crew_instance
    # ✅ Start background agents to fetch data and monitor
    # def start_background_agents(self):
    #     """Start background tasks for data fetching and monitoring."""
    #     threading.Thread(target=self.run_data_fetch_agent, daemon=True).start()
        # threading.Thread(target=self.run_monitoring_agent, daemon=True).start()

    # ✅ Fetch data continuously in the background
    # def run_data_fetch_agent(self):
    #     """Fetch data and store it in the shared state."""
    #     while True:
    #         print("🔄 Fetching data in the background...")
    #         all_patient_data = monitor_all_patients()  # Fetch and monitor all patients
    #         self.shared_data["patient_data"] = all_patient_data
    #         print(f"✅ Data updated: {self.shared_data['patient_data']}")
    #         time.sleep(5)  # Fetch data every 5 seconds

    # ✅ Monitor and trigger alerts based on fetched data
    # def run_monitoring_agent(self):
    #     """Monitor and update alerts dynamically."""
    #     while True:
    #         if "patient_data" in self.shared_data:
    #             print("🔍 Monitoring patient vitals...")
    #             monitored_data = monitor_all_patients()
    #             self.shared_data["alerts"] = monitored_data
    #             print(f"🚨 Alerts updated: {self.shared_data['alerts']}")
    #         time.sleep(5)

    # ✅ User query to fetch real-time status and alert data
    def get_patient_data(self, patient_id: int, parameter:str) -> str:
        """Fetch the latest patient data and alert status dynamically."""
        # patient_data = self.shared_data.get("patient_data", {}).get(patient_id, None)
        # alerts = self.shared_data.get("alerts", {}).get(patient_id, [])
        patient_data = fetch_track_data(patient_id,parameter)

        if not patient_data:
            return "❌ No data available for this patient."

        response = {
            "summary": f"Patient {patient_id} vitals summary: {patient_data}",
            # "alert_details": alerts,
            # "next_steps": self.generate_next_steps(alerts),
        }

        return response


    # ✅ Generate next steps dynamically based on alert severity
    # def generate_next_steps(self, alerts):
    #     """Generate suggestions based on alert severity."""
    #     if not alerts:
    #         return "✅ No critical alerts. Continue monitoring."
    #     critical_alerts = [alert for alert in alerts if alert["severity"] == "high"]
    #     if critical_alerts:
    #         return "⚠️ Critical condition detected! Recommend immediate ICU admission."
    #     moderate_alerts = [alert for alert in alerts if alert["severity"] == "moderate"]
    #     if moderate_alerts:
    #         return "🩺 Moderate concerns. Suggest notifying the physician for evaluation."
    #     return "🔍 Low severity. Continue monitoring with periodic checks."
    def process_query(self, query):
        """Processes natural language query and sends it to the correct agent."""
        try:
            # ✅ Parse the query correctly
            parsed_query = self.query_parser.parse_query(query)

            task = parsed_query.get("task")
            patient_id = parsed_query.get("patient_id")
            parameter = parsed_query.get("parameter")

            if not task or not patient_id:
                return "❗ Invalid query. Please provide a valid task and patient ID."

            if task == "fetch_data":
                # ✅ Fetch only the latest data dynamically from shared state
                latest_data = self.get_patient_data(int(patient_id),parameter)
                if isinstance(latest_data, str):
                    return latest_data
                return latest_data

            elif task == "check_alert":
                # ✅ Fetch latest alert info dynamically
                alert_data = self.get_patient_data(int(patient_id))
                if isinstance(alert_data, str):
                    return alert_data
                return alert_data.get("alert_details", "No alerts found.")
            
            else:
                return "❗ Sorry, I don't support that query type yet. I can fetch data or check alerts."

        except Exception as e:
            return f"❗ Error processing query: {str(e)}"
