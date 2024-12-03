# # Use the official Apache Airflow image as a base
# FROM apache/airflow:2.7.2-python3.10

# # Copy DAGs
# COPY dags/ /opt/airflow/dags/

# # Set Working Directory
# WORKDIR /opt/airflow

# # Install additional Python dependencies
# COPY requirements.txt /requirements.txt

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
#USER airflow

# Use the official Apache Airflow image as a base
FROM apache/airflow:2.7.2-python3.10

# Copy DAGs
COPY dags/ /opt/airflow/dags/

# Set Working Directory
WORKDIR /opt/airflow

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Switch to root for managing directories and permissions
USER root

# Create logs directory and set permissions (this must be done as root)
RUN mkdir -p /opt/airflow/logs && chmod -R 777 /opt/airflow/logs

# Create airflow user and group (if not already created)
#RUN groupadd -r airflow && useradd -r -g airflow airflow

# Switch to airflow user to install dependencies
USER airflow

# Install Python dependencies as the airflow user
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir dvc[s3]  # Install DVC with optional S3 support

# Switch back to root to handle the permissions for the logs
USER root

# Ensure airflow user can access logs
RUN chown -R airflow:airflow /opt/airflow/logs

# Switch to airflow user for running Airflow processes
USER airflow

# Copy entrypoint.sh and make it executable
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh
