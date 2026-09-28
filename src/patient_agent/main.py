import sys
import warnings
import threading
import time
from datetime import datetime
from patient_agent.crew import PatientAgent

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")


def valid_patient_id(input_text):
    """Simple validator for patient ID - checks if input is numeric."""
    try:
        patient_id = int(input_text.strip())
        return patient_id
    except (ValueError, TypeError):
        return None


def start_monitoring(agent, patient_id):
    """Run CrewAI monitoring in the background."""
    try:
        inputs = {"patient_id": patient_id}
        response = agent.crew().kickoff(inputs=inputs)
        print("🤖 Initial Monitoring Response:", response)
    except Exception as e:
        print(f"❗ An error occurred while running the monitoring: {e}")


def run():
    """Run the crew with proper input validation and threaded monitoring."""
    try:
        print("🚀 Starting Patient Monitoring System")
        
        # Initialize PatientAgent
        print("🔄 Initializing PatientAgent...")
        patient_agent = PatientAgent()
        print("✅ PatientAgent initialized successfully")
        
        # Ask for valid patient ID
        patient_id = None
        while patient_id is None:
            print("\n" + "=" * 70)
            print("👨‍⚕️ PATIENT ID REQUIRED - ENTER A NUMERIC ID AND PRESS ENTER:")
            print("=" * 70)
            sys.stdout.flush()
            
            try:
                user_input = input("> ")
                patient_id = valid_patient_id(user_input)
                if patient_id is None:
                    print("❌ Invalid Patient ID format. Please enter a valid number.")
                    continue
            except KeyboardInterrupt:
                print("\n⛔ Process terminated by user.")
                return
        
        print(f"✅ Patient ID {patient_id} accepted.")
        print("🔄 Preparing monitoring system...")
        
        # Start background monitoring without waiting for it to complete
        def background_monitor():
            try:
                start_monitoring(patient_agent, patient_id)
            except Exception as e:
                print(f"❗ Monitoring error: {e}")
        
        # Start monitoring in a true background thread
        monitor_thread = threading.Thread(target=background_monitor, daemon=True)
        monitor_thread.start()
        print("✅ Background monitoring started for patient ID:", patient_id)
        
        # Start query interface in the main thread
        print("\n" + "=" * 70)
        print(f"🔍 MONITORING ACTIVE - YOU CAN NOW ASK QUESTIONS ABOUT PATIENT {patient_id}")
        print("=" * 70)
        
        # Query loop
        while True:
            try:
                print("\n🔎 Ask about the patient (or type 'exit' to quit): ", end='')
                sys.stdout.flush()
                query = input()
                
                if query.lower() == 'exit':
                    print("👋 Exiting... Goodbye!")
                    break
                
                # Process query
                if hasattr(patient_agent, "process_query"):
                    print("🔄 Processing query...")
                    response = patient_agent.process_query(query)
                    print(f"\n🤖 Response: {response}")
                else:
                    print("⚠️ Query processing not available.")
                    
            except KeyboardInterrupt:
                print("\n⛔ Process terminated by user.")
                break
            except Exception as e:
                print(f"❗ Query error: {e}")
    
    except Exception as e:
        print(f"❗ An error occurred: {e}")


if __name__ == "__main__":
    run()