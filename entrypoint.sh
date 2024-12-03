#!/bin/bash

# Set permissions for the logs directory
chmod -R 775 /opt/airflow/logs
chown -R airflow:airflow /opt/airflow/logs
# Initialize the Airflow database
airflow db init

# Create an Airflow user (if not already created)
airflow users create --username smp1699 --firstname Mrudula --lastname Acharya --role Admin --email mrudulaacharya18@gmail.com --password smp1699


airflow scheduler &

sleep 60
# Start the webserver in the foreground (so Docker keeps the container running)
exec airflow webserver
