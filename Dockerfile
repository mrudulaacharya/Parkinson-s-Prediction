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

# Set Working Directory
WORKDIR /opt/airflow

# Copy DAGs and requirements file
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt

# Install Python dependencies as airflow user
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir dvc[s3]  # Install DVC with optional S3 support


# Switch to root to allow modification of directories
USER root
#RUN chmod -R 775 /opt/airflow/logs

# Ensure entrypoint.sh is executable
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

# Switch back to airflow user for running Airflow processes
USER airflow
