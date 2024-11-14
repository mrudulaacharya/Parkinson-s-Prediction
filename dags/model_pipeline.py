import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'model_pipeline',
    default_args=default_args,
    description='Model pipeline with custom email alerts for task failures',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Custom email alert function
def send_custom_alert_email(**context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    try_number = context['task_instance'].try_number
    subject = "Airflow Task Alert - Failure or Retry"
    body = f"Task {task_id} in DAG {dag_id} has failed or retried (Attempt: {try_number})."
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "mrudulaacharya18@gmail.com"
    msg['To'] = "mrudulaacharya18@gmail.com"
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("mrudulaacharya18@gmail.com", "lhwnkkhmvptmjghx")  # Use an app-specific password
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
            print("Alert email sent successfully.")
    except Exception as e:
        print(f"Error sending alert email: {e}")


def get_data_from_data_pipeline(**context):
    #df = context['dag_run'].conf['processed_data']
    df=pd.read_csv('/home/mrudula/MLPOPS/outputs/airflow_cleaned_data.csv')
    context['ti'].xcom_push(key='df', value=df)
    return df
    
def split_to_X_y(**context):
    df=context['ti'].xcom_pull(key='df', task_ids='get_data_from_data_pipeline')
    if df is not None:
        # Convert JSON string to DataFrame
        df = pd.read_json(df)
    
    logging.info(f"Retrieved df from XCom: {df}")
    y=df['COHORT']
    X=df.drop(columns=['COHORT'])
    context['ti'].xcom_push(key='y', value=y)
    context['ti'].xcom_push(key='X', value=X)
    return X,y

def train_test_split(**context):
    X= context['ti'].xcom_pull(key='X', task_ids='split_to_X_y')
    y= context['ti'].xcom_pull(key='y', task_ids='split_to_X_y')
    # Split data into train/test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    context['ti'].xcom_push(key='y_train', value=y_train)
    context['ti'].xcom_push(key='X_train', value=X_train)
    context['ti'].xcom_push(key='y_test', value=y_test)
    context['ti'].xcom_push(key='X_test', value=X_test)

    return X_train,y_train,X_test,y_test




    

get_data_from_data_pipeline_task = PythonOperator(
    task_id='get_data_from_data_pipeline',
    python_callable= get_data_from_data_pipeline,
    provide_context=True,
    dag=dag,
)

split_to_X_y_task = PythonOperator(
    task_id='split_to_X_y',
    python_callable=split_to_X_y,
    provide_context=True,
    dag=dag,
)

train_test_split_task = PythonOperator(
    task_id='train_test_split',
    python_callable=train_test_split,
    provide_context=True,
    dag=dag,
)

# task_clean_demographics = PythonOperator(
#     task_id='task_clean_demographics',
#     python_callable=clean_demographics,
#     provide_context=True,
#     dag=dag,
# )


# Send alert email in case of failure
task_send_alert_email = PythonOperator(
    task_id='task_send_alert_email',
    python_callable=send_custom_alert_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

get_data_from_data_pipeline_task>>split_to_X_y_task>>train_test_split_task>>task_send_alert_email