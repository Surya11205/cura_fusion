import pathway as pw
import pathway.io.kafka as pw_kafka

# Define Case Tracking Schema
case_tracking_schema = pw.schema_from_dict({
    "caseid": str,
    "tname": str,
    "tid": str,
    "time": float,
    "value": float,
    "age": int,
    "sex": str,
    "department": str,
    "lab_results": dict,
})


# Kafka settings
kafka_settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "case_tracking_consumer_group",
    "auto.offset.reset": "earliest",
}


def read_case_tracking_data():
    """Reads case tracking data from Kafka and returns the stream."""
    return pw_kafka.read(
        kafka_settings,
        topic="case_tracking",
        schema=case_tracking_schema,
        format="json",
        autocommit_duration_ms=1000,
    )


def process_case_tracking_data(case_tracking_stream):
    """Processes case tracking data and returns abnormal cases."""
    # Detecting abnormal signal values

    abnormal_cases = case_tracking_stream.filter(pw.this.value > 100)

    # Prepare alert messages
    processed_cases = abnormal_cases.select(
        caseid=pw.this.caseid,
        tname=pw.this.tname,
        alert="High Signal Detected",
        time=pw.this.time,
    )

    # Print for debugging (replace with Streamlit later)
    processed_cases.debug(name="All Abnormal Cases")

    return processed_cases


def process_specific_case(case_tracking_stream, target_caseid):
    """Processes data and returns alerts for a specific case."""
    specific_case_stream = case_tracking_stream.filter(case_tracking_stream.caseid == target_caseid)

    # Detecting abnormal signal values for the specific case
    abnormal_specific_case = specific_case_stream.filter(specific_case_stream.value > 100)

    # Prepare alert messages
    specific_case_alerts = abnormal_specific_case.select(
        caseid=pw.this.caseid,
        tname=pw.this.tname,
        alert="High Signal Detected for Specific Case",
        time=pw.this.time,
    )

    # Print for debugging (replace with Streamlit later)
    specific_case_alerts.debug(name=f"Abnormal Cases for Case ID {target_caseid}")

    return specific_case_alerts


def write_processed_data_to_kafka(processed_cases, topic_name="processed_case_tracking_topic"):
    """Writes processed case tracking data back to Kafka."""
    pw_kafka.write(
        rdkafka_settings=kafka_settings,
        table=processed_cases,
        topic_name=topic_name,
        format="json",
    )


def run_case_tracking_pipeline():
    """Runs the case tracking pipeline for all cases."""
    case_tracking_stream = read_case_tracking_data()
    processed_cases = process_case_tracking_data(case_tracking_stream)
    write_processed_data_to_kafka(processed_cases)
    pw.run()


def run_specific_case_pipeline(target_caseid):
    """Runs the pipeline for a specific case."""
    case_tracking_stream = read_case_tracking_data()
    specific_case_alerts = process_specific_case(case_tracking_stream, target_caseid)
    write_processed_data_to_kafka(specific_case_alerts, topic_name="specific_case_alerts_topic")
    pw.run()


if __name__ == "__main__":
    print("🚀 Starting Case Tracking Pipeline...")
    run_case_tracking_pipeline()
