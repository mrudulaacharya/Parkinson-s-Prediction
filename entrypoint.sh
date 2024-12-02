#!/bin/bash
# Initialize DVC if not already done
# if [ ! -d "/opt/airflow/.dvc" ]; then
#   echo "Initializing DVC..."
#   dvc init
# fi

# # Add datasets to DVC tracking
# if [ -d "/opt/airflow/raw_data" ]; then
#   dvc add /opt/airflow/raw_data
# fi

# if [ -d "/opt/airflow/motor_assessments" ]; then
#   dvc add /opt/airflow/motor_assessments
# fi
# # Log the cleaned data if it exists
# if [ -f "/opt/airflow/outputs/airflow_cleaned_data.csv" ]; then
#   dvc add /opt/airflow/outputs/airflow_cleaned_data.csv
# fi

# # Commit DVC changes to Git
# git add /opt/airflow/raw_data.dvc /opt/airflow/motor_assessments.dvc
# git commit -m "Track data using DVC"


# Initialize the Airflow database
airflow db init

# Create an Airflow user (if not already created)
airflow users create --username smp1699 --firstname Mrudula --lastname Acharya --role Admin --email mrudulaacharya18@gmail.com --password smp1699

# # Ensure Git is initialized and synced for DVC
# if [ ! -d "/opt/airflow/.git" ]; then
#   echo "Initializing Git repository for DVC..."
#   git init
#   git config --global user.name "Airflow DVC Logger"
#   git config --global user.email "shalakapkar@gmail.com"
#   git add .
#   git commit -m "Initial commit for Airflow and DVC setup"
# fi
# Start the scheduler in the background
chmod -R 777 /opt/airflow/logs

airflow scheduler &

sleep 60
# Start the webserver in the foreground (so Docker keeps the container running)
exec airflow webserver
