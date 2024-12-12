FROM apache/airflow:2.10.3-python3.9

# USER root

COPY requirements.txt /requirements.txt
# Install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
  libpq-dev gcc python3-dev && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
USER airflow
# RUN pip install psycopg2-binary
RUN pip install --no-cache-dir -r /requirements.txt