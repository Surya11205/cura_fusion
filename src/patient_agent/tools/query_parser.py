import json
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain.chains import LLMChain


class QueryParser:
    """Parses natural language queries and converts them to structured queries for the patient data tool."""

    def __init__(self):
        # ✅ Using Google Gemini (gemini-2.0-flash) with API key
        self.model = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key="AIzaSyBIzVjmYvWP0NYG92UkIPwZWD__exss69w",
        )

    def parse_query(self, query):
        """Parse query and extract intent."""
        prompt_template = """
        You are a query parsing assistant. Convert the natural language query into a structured format.

        Query: "{query}"
        Expected Format: {{"task": "fetch_data | check_alert | get_trend", "patient_id": "<patient_id>", "parameter": "<parameter_name>"}}
        """
        prompt = PromptTemplate(template=prompt_template, input_variables=["query"])
        llm_chain = LLMChain(llm=self.model, prompt=prompt)

        # ✅ Run with user query
        parsed_output = llm_chain.invoke({"query": query})
        raw_text = parsed_output["text"]

# Clean the string - Remove triple backticks and 'json' prefix
        clean_response = raw_text.replace("```json", "").replace("```", "").strip()


        try:
            # print("Raw Gemini Output:", parsed_output)
            # response = parsed_output.strip()
            # clean_response = response.strip("`").replace("json", "").strip()
            # 🔥 Convert the response to a dictionary
            parsed_query = json.loads(clean_response)
            print(parsed_query,type(parsed_query))
            return parsed_query
        except json.JSONDecodeError as e:
            print(f"❗ Error decoding response: {e}")
            return {"task": None, "patient_id": None, "parameter": None}
