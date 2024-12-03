#!/bin/bash

chmod -R 777 /opt/airflow/logs
chmod -R 777 /opt/airflow/raw_data
chmod -R 777 /opt/airflow/motor_assessments
chmod -R 777 /opt/airflow/outputs
chmod -R 777 /opt/airflow/models
chmod -R 777 /opt/airflow/mlruns
chmod -R 777 /opt/airflow/.dvc
# Initialize the Airflow database
airflow db init

# Create an Airflow user (if not already created)
airflow users create --username smp1699 --firstname Mrudula --lastname Acharya --role Admin --email mrudulaacharya18@gmail.com --password smp1699


airflow scheduler &

sleep 60
# Start the webserver in the foreground (so Docker keeps the container running)
exec airflow webserver
