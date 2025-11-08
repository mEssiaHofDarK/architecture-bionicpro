FROM apache/airflow:3.1.1-python3.12
USER root

COPY requirements.txt ./requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt