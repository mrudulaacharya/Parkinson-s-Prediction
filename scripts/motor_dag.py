from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 1)
}

# Directory where CSV files are stored
csv_directory = '/home/mrudula/MLPOPS/motor_senses/'

# Load functions for each CSV file
def load_motor_senses_1():
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_I_27Oct2024.csv'))

def load_motor_senses_2():
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_I_Patient_Questionnaire_27Oct2024.csv'))

def load_motor_senses_3():
    return pd.read_csv(os.path.join(csv_directory, 'MDS_UPDRS_Part_II__Patient_Questionnaire_27Oct2024.csv'))

def load_motor_senses_4():
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_III_27Oct2024.csv'))

def load_motor_senses_5():
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_IV__Motor_Complications_27Oct2024.csv'))

# Clean functions for each loaded CSV
def clean_motor_senses_1(df):
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP1RTOT'])

def clean_motor_senses_2(df):
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP1PTOT'])

def clean_motor_senses_3(df):
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP2PTOT'])

def clean_motor_senses_4(df):
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','PDTRTMNT','PDSTATE','HRPOSTMED','HRDBSON','HRDBSOFF','PDMEDYN','DBSYN','ONOFFORDER','OFFEXAM','OFFNORSN','DBSOFFTM','ONEXAM','ONNORSN','DBSONTM','PDMEDDT','PDMEDTM','EXAMDT','EXAMTM','NP3TOT'])

def clean_motor_senses_5(df):
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP4TOT'])

# Function to merge all cleaned CSVs
def merge_all_motor_senses_csvs(**context):
    # Pull cleaned DataFrames from XCom
    cleaned_dfs = [
        context['ti'].xcom_pull(task_ids='clean_csv_1_task'),
        context['ti'].xcom_pull(task_ids='clean_csv_2_task'),
        context['ti'].xcom_pull(task_ids='clean_csv_3_task'),
        context['ti'].xcom_pull(task_ids='clean_csv_4_task'),
        context['ti'].xcom_pull(task_ids='clean_csv_5_task')
    ]
    
    # Merge all DataFrames (concatenation along rows)
    merged_df = pd.concat(cleaned_dfs, axis=0)
    
    # Save the merged DataFrame to a CSV file
    merged_path = os.path.join(csv_directory, 'merged_file.csv')
    merged_df.to_csv(merged_path, index=False)
    
    print(f"Merged file saved at {merged_path}")

# Define the DAG
with DAG('load_clean_merge_csvs',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Load tasks
    load_motor_senses_1_task = PythonOperator(
        task_id='load_motor_senses_1_task',
        python_callable=load_motor_senses_1
    )

    load_motor_senses_2_task = PythonOperator(
        task_id='load_motor_senses_2_task',
        python_callable=load_motor_senses_2
    )

    load_motor_senses_3_task = PythonOperator(
        task_id='load_motor_senses_3_task',
        python_callable=load_motor_senses_3
    )

    load_motor_senses_4_task = PythonOperator(
        task_id='load_motor_senses_4_task',
        python_callable=load_motor_senses_4
    )

    load_motor_senses_5_task = PythonOperator(
        task_id='load_motor_senses_5_task',
        python_callable=load_motor_senses_5
    )

    # Clean tasks
    clean_motor_senses_1_task = PythonOperator(
        task_id='clean_motor_senses_1_task',
        python_callable=clean_motor_senses_1,
        op_args=['{{ ti.xcom_pull(task_ids="load_csv_1_task") }}']
    )

    clean_motor_senses_2_task = PythonOperator(
        task_id='clean_motor_senses_2_task',
        python_callable=clean_motor_senses_2,
        op_args=['{{ ti.xcom_pull(task_ids="load_csv_2_task") }}']
    )

    clean_motor_senses_3_task = PythonOperator(
        task_id='clean_motor_senses_3_task',
        python_callable=clean_motor_senses_3,
        op_args=['{{ ti.xcom_pull(task_ids="load_csv_3_task") }}']
    )

    clean_motor_senses_4_task = PythonOperator(
        task_id='clean_motor_senses_4_task',
        python_callable=clean_motor_senses_4,
        op_args=['{{ ti.xcom_pull(task_ids="load_csv_4_task") }}']
    )

    clean_motor_senses_5_task = PythonOperator(
        task_id='clean_motor_senses_5_task',
        python_callable=clean_motor_senses_5,
        op_args=['{{ ti.xcom_pull(task_ids="load_csv_5_task") }}']
    )

    # Merge task
    merge_all_motor_senses_csvs_task = PythonOperator(
        task_id='merge_all_motor_senses_csvs_task',
        python_callable=merge_all_motor_senses_csvs,
        provide_context=True
    )

    # Define task dependencies
    # Load -> Clean -> Merge
    [load_motor_senses_1_task >> clean_motor_senses_1_task,
     load_motor_senses_2_task >> clean_motor_senses_2_task,
     load_motor_senses_3_task >> clean_motor_senses_3_task,
     load_motor_senses_4_task >> clean_motor_senses_4_task,
     load_motor_senses_5_task >> clean_motor_senses_5_task] >> merge_task