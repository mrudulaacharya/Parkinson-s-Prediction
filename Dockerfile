# Use the official Apache Airflow image as a base
FROM apache/airflow:2.7.2-python3.10

# Copy DAGs
COPY dags/ /opt/airflow/dags/

# Set Working Directory
WORKDIR /opt/airflow

# Install additional Python dependencies
COPY requirements.txt /requirements.txt

# Switch to root, set permissions, then switch back to airflow user
USER airflow

RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir dvc[s3]  # Install DVC with optional S3 support

USER root

# Create the airflow user and group manually
RUN groupadd -r airflow && useradd -r -g airflow airflow
RUN apt-get update && apt-get install -y git 
RUN mkdir -p /opt/airflow/logs && chown -R airflow:airflow /opt/airflow/logs && chmod -R 775 /opt/airflow/logs
# Ensure logs directory has the right permissions
#RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh
USER airflow