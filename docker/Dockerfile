FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk

COPY --chown=airflow:airflow requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"