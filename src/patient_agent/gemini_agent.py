# from langchain.llms import GoogleGenerativeAI
# from langchain.chains import LLMChain
# from langchain.prompts import PromptTemplate
# from confluent_kafka import Producer

# # 🔥 Configure Gemini API
# GEMINI_API_KEY = "AIzaSyBIzVjmYvWP0NYG92UkIPwZWD__exss69w"
# llm = GoogleGenerativeAI(api_key=GEMINI_API_KEY)

# # 🔥 Define the Prompt Template
# alert_prompt_template = """
# You are a healthcare assistant analyzing patient data.
# Evaluate the following alert and provide a recommendation.

# Alert Details:
# - Case ID: {caseid}
# - Parameter: {tname}
# - Value: {value} {unit}
# - Expected Range: {min_value} - {max_value} {unit}
# - Department: {department}

# Provide a brief and actionable recommendation.
# """

# prompt = PromptTemplate(input_variables=["caseid", "tname", "value", "min_value", "max_value", "unit", "department"], template=alert_prompt_template)

# # 🔥 Create Gemini Chain
# alert_chain = LLMChain(llm=llm, prompt=prompt)

# # 🔹 Kafka Configuration for Sending Results
# KAFKA_BROKER = "localhost:9092"
# RESULT_TOPIC = "alert_results"

# producer = Producer({"bootstrap.servers": KAFKA_BROKER})

# def send_result(caseid, recommendation):
#     """Send recommendation to Kafka result topic."""
#     result_message = {"caseid": caseid, "recommendation": recommendation}
#     producer.produce(RESULT_TOPIC, key=caseid, value=str(result_message))
#     producer.flush()

# def process_alert(alert):
#     """Process alert and generate recommendation."""
#     response = alert_chain.run(alert)
#     print(f"🎯 Recommendation: {response}")

#     # 🔥 Send result to Kafka
#     send_result(alert["caseid"], response)
