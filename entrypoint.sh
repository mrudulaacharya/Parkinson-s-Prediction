#!/bin/bash

set -e
echo "Authenticating with GCP..."
gcloud auth activate-service-account --key-file=/opt/airflow/sa-key.json

airflow db init

# Create an Airflow user (if not already created)
airflow users create --username smp1699 --firstname Mrudula --lastname Acharya --role Admin --email mrudulaacharya18@gmail.com --password smp1699

echo "Creating local directories..."
mkdir -p /opt/airflow/raw_data
mkdir -p /opt/airflow/motor_assessment
mkdir -p /opt/airflow/models
mkdir -p /opt/airflow/mlruns
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/outputs
echo "Syncing data from GCP..."

# Sync raw data
gsutil -m rsync -r gs://parkinsons_prediction_data_bucket/raw_data /opt/airflow/raw_data
gsutil -m rsync -r gs://parkinsons_prediction_data_bucket/motor_assessment /opt/airflow/motor_assessment

# Sync mount folders
#gsutil rsync -r gs://your-bucket-name/outputs /opt/airflow/outputs
gsutil -m rsync -r gs://parkinsons_prediction_data_bucket/models /opt/airflow/models
gsutil -m rsync -r gs://parkinsons_prediction_data_bucket/mlruns /opt/airflow/mlruns
gsutil -m rsync -r gs://parkinsons_prediction_data_bucket/logs /opt/airflow/logs
gsutil -m rsync -r gs://parkinsons_prediction_data_bucket/outputs /opt/airflow/outputs

echo "Data sync complete!"



airflow scheduler &

sleep 60
# Start the webserver in the foreground (so Docker keeps the container running)
exec airflow webserver
