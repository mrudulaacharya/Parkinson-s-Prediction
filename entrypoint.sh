#!/bin/bash

# Initialize the Airflow database
airflow db init

# Create an Airflow user (if not already created)
airflow users create --username smp1699 --firstname Shalaka --lastname Padalkar --role Admin --email shalakapkar@gmail.com --password smp1699

# Start the scheduler in the background
airflow scheduler &

# Start the webserver in the foreground (so Docker keeps the container running)
exec airflow webserver
