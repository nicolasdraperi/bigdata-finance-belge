FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb wget gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir requests beautifulsoup4 lxml hdfs playwright pyvirtualdisplay pymongo stem pysocks
USER root
RUN /home/airflow/.local/bin/playwright install-deps chromium
USER airflow
RUN playwright install chromium