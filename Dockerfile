# Use the official Apache Airflow image as a base
FROM apache/airflow:2.7.2-python3.10

# Copy DAGs
COPY dags/ /opt/airflow/dags/

# Set Working Directory
WORKDIR /opt/airflow

# Install additional Python dependencies
COPY requirements.txt /requirements.txt

# Switch to root, set permissions, then switch back to airflow user
# USER airflow

# RUN pip install --no-cache-dir -r /requirements.txt
# RUN pip install --no-cache-dir dvc[s3]  # Install DVC with optional S3 support

# USER root


# RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs
# RUN apt-get update && apt-get install -y git 

# # Ensure logs directory has the right permissions
# #RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs
# COPY entrypoint.sh /opt/airflow/entrypoint.sh
# RUN chmod +x /opt/airflow/entrypoint.sh
# Switch to root for installing dependencies
USER root

RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir dvc[s3]  # Install DVC with optional S3 support

# Install git (if needed)
RUN apt-get update && apt-get install -y git

# Ensure logs directory exists and has proper permissions
RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs

# Ensure the Airflow user can write to logs directory
RUN chown -R airflow:airflow /opt/airflow/logs

# Copy entrypoint.sh and make it executable
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

USER airflow