import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'mrudulaacharya18@gmail.com',  # Replace with your email address
}

# Define the pipeline DAG``
dag = DAG(
    'data_pipeline_with_email_alerts',
    default_args=default_args,
    description='Data pipeline with email alerts for data irregularities',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Task 1: Load and merge data
def load_and_merge_data(**kwargs):
    directory_path = '/home/mrudula/MLPOPS'
    dataframes = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            dataframes.append(df)
    merged_df = dataframes[0]
    for df in dataframes[1:]:
        merged_df = merged_df.merge(df, on=['PATNO'], how='inner')
    output_path = '/home/mrudula/MLPOPS/merged__m_output.csv'
    merged_df.to_csv(output_path, index=False)
    return output_path  # Return the output file path

load_and_merge_task = PythonOperator(
    task_id='load_and_merge_data',
    python_callable=load_and_merge_data,
    dag=dag,
)

# Task 2: Drop columns
def drop_columns(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='load_and_merge_data')  # Get the input path from Task 1
    df = pd.read_csv(input_path)
    columns_to_drop = [
        'ENROLL_DATE', 'EVENT_ID_x', 'PAG_NAME_x', 'INFODT_x', 'PATNO',
        'RUNDATE', 'PROJECTID', 'EVENT_ID_y', 'INFODT_y', 'ORIG_ENTRY',
        'LAST_UPDATE', 'ORIG_ENTRY.1', 'LAST_UPDATE.1', 'ORIG_ENTRY.2',
        'LAST_UPDATE.2', 'PDMEDDT', 'PDMEDTM', 'EXAMDT', 'EXAMTM', 'ORIG_ENTRY.3',
        'LAST_UPDATE.3', 'REC_ID', 'PAG_NAME_y'
    ]
    df = df.drop(columns=columns_to_drop)
    output_path = '/home/mrudula/MLPOPS/data_modified.csv'
    df.to_csv(output_path, index=False)
    return output_path  # Return the output file path

drop_columns_task = PythonOperator(
    task_id='drop_columns',
    python_callable=drop_columns,
    provide_context=True,  # Needed to use `kwargs`
    dag=dag,
)

# Task 3: Data cleaning, preprocessing, and EDA
def clean_preprocess_eda(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='drop_columns')  # Get the input path from Task 2
    df = pd.read_csv(input_path)

    # Data Cleaning
    df = df.drop_duplicates()
    drop_threshold = 0.9 * len(df)
    missing_values = df.isnull().sum()
    columns_to_drop = missing_values[missing_values > drop_threshold].index.tolist()
    df = df.drop(columns=columns_to_drop)
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            df[col].fillna(int(df[col].mean()), inplace=True)
        elif df[col].dtype == 'object':
            df[col].fillna(df[col].mode()[0], inplace=True)
    df.to_csv('/home/mrudula/MLPOPS/cleaned_data.csv', index=False)

    # Data Preprocessing
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    categorical_cols = df.select_dtypes(include=['object']).columns
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

    # Check for irregularities
    if df.isnull().any().any():
        raise ValueError("Data irregularities detected: missing values present")

clean_preprocess_eda_task = PythonOperator(
    task_id='clean_preprocess_eda',
    python_callable=clean_preprocess_eda,
    provide_context=True,
    dag=dag,
)

# Email alert task
email_alert_task = EmailOperator(
    task_id='send_email_alert',
    to='mrudulaacharya18@gmail.com',  # Replace with actual recipient email
    subject='Data Irregularities Detected',
    html_content="<h3>Data irregularities were detected during data cleaning and preprocessing.</h3>",
    trigger_rule='one_failed',  # Send email only if a task fails
    dag=dag,
)

# Set up task dependencies
load_and_merge_task >> drop_columns_task >> clean_preprocess_eda_task >> email_alert_task
