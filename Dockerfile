# Use the official Apache Airflow image as a base
FROM apache/airflow:2.7.2-python3.10

# Set Working Directory
WORKDIR /opt/airflow

# Copy DAGs and requirements file
COPY dags/ /opt/airflow/dags/
COPY docker_deploy/ /opt/airflow/docker_deploy
COPY config.json /opt/airflow/config.json
COPY requirements.txt /requirements.txt

# Install Python dependencies as airflow user
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir dvc[all]  # Install DVC with optional S3 support

# Switch to root to allow modification of directories
USER root

RUN chmod -R 775 /opt/airflow/docker_deploy 

RUN apt-get update && apt-get install -y \
    curl apt-transport-https ca-certificates gnupg

# Add Google Cloud SDK distribution URI as a package source
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee /etc/apt/sources.list.d/google-cloud-sdk.list

# Add the public key for the Google Cloud SDK repository
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg

# Update package list and install Google Cloud SDK along with gke-gcloud-auth-plugin
RUN apt-get update && apt-get install -y \
    google-cloud-sdk \
    google-cloud-sdk-gke-gcloud-auth-plugin

# Clean up unnecessary files to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY ./sa-key.json /opt/airflow/sa-key.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/sa-key.json

ENV PATH $PATH:/usr/lib/google-cloud-sdk/bin

# Install Docker
RUN apt-get update && apt-get install -y \
    docker.io && \
    rm -rf /var/lib/apt/lists/*

# Install kubectl
RUN apt-get update && apt-get install -y curl && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && mv kubectl /usr/local/bin/


# Ensure entrypoint.sh is executable
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

# Switch back to airflow user for running Airflow processes
USER airflow
