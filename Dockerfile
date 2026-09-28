# Use official Python base image
FROM python:3.12.3

# Set working directory in container
WORKDIR /app

# Copy requirements file first (to leverage Docker cache)
COPY src/patient_agent/requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files into the container
COPY . /app/

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "src/patient_agent/gpt_stream_lit.py"]
