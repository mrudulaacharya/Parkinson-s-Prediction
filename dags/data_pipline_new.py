import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_with_custom_email_alerts1',
    default_args=default_args,
    description='Data pipeline with custom email alerts for task failures',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define file paths
participant_status_path = '/home/mrudula/MLPOPS/data_raw/Participant_Status_27Oct2024.csv'
demographics_path = '/home/mrudula/MLPOPS/data_raw/Demographics_27Oct2024.csv'
biospecimen_analysis_path = '/home/mrudula/MLPOPS/data_raw/SAA_Biospecimen_Analysis_Results_27Oct2024.csv'

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

# Define functions for each data processing task
def participant_status_load(**context):
    participant_status = pd.read_csv(participant_status_path)
    context['ti'].xcom_push(key='participant_status', value=participant_status)
    return participant_status

def demographics_load(**context):
    demographics = pd.read_csv(demographics_path)
    context['ti'].xcom_push(key='demographics', value=demographics)
    return demographics

def clean_participant_status(**context):
    participant_status = context['ti'].xcom_pull(task_ids='task_participant_status_load', key='participant_status')
    participant_status['ENROLL_DATE'] = pd.to_datetime(participant_status['ENROLL_DATE'], format='%m/%Y', errors='coerce')
    participant_status = participant_status.rename(columns={"PATNO": "Participant_ID"})
    columns_to_drop = ['COHORT_DEFINITION','STATUS_DATE','INEXPAGE','AV133STDY','TAUSTDY','GAITSTDY','PISTDY','SV2ASTDY','PPMI_ONLINE_ENROLL']
    participant_status.drop(columns=columns_to_drop, inplace=True)
    context['ti'].xcom_push(key='cleaned_participant_status', value=participant_status)
    return participant_status

def clean_demographics(**context):
    demographics = context['ti'].xcom_pull(task_ids='task_demographics_load', key='demographics')
    columns_to_drop = ['REC_ID','EVENT_ID','PAG_NAME','INFODT','AFICBERB','ASHKJEW','BASQUE','BIRTHDT','HOWLIVE', 
                       'GAYLES', 'HETERO','BISEXUAL','PANSEXUAL','ASEXUAL', 'OTHSEXUALITY','ORIG_ENTRY','LAST_UPDATE']
    demographics.drop(columns=columns_to_drop, inplace=True)
    context['ti'].xcom_push(key='cleaned_demographics', value=demographics)
    return demographics

def merge_participant_status_and_demographics(**context):
    participant_status = context['ti'].xcom_pull(task_ids='task_clean_participant_status', key='cleaned_participant_status')
    demographics = context['ti'].xcom_pull(task_ids='task_clean_demographics', key='cleaned_demographics')
    combined_table = pd.merge(
        participant_status,
        demographics,
        left_on="Participant_ID",
        right_on="PATNO",
        suffixes=("", "_drop")
    )
    valid_statuses = ['Enrolled', 'Complete', 'Withdrew']
    combined_table = combined_table[combined_table['ENROLL_STATUS'].isin(valid_statuses)]
    context['ti'].xcom_push(key='combined_table', value=combined_table)
    return combined_table

def clean_participantstatus_demographic(**context):
    combined_table = context['ti'].xcom_pull(task_ids='task_merge_participant_status_and_demographics', key='combined_table')
    columns_to_drop = ['ENROLL_STATUS','PATNO','ENRLLRRK2','ENRLPINK1','HANDED','HISPLAT','RAASIAN','RABLACK','RAHAWOPI',
                       'RAINDALS','RANOS','RAWHITE','RAUNKNOWN']
    combined_table.drop(columns=columns_to_drop, inplace=True)
    context['ti'].xcom_push(key='cleaned_participantstatus_demographic', value=combined_table)
    return combined_table
def biospecimen_analysis_load(**context):
    biospecimen_analysis = pd.read_csv(biospecimen_analysis_path)
    context['ti'].xcom_push(key='biospecimen_analysis', value=biospecimen_analysis)
    return biospecimen_analysis

def clean_biospecimen_analysis(**context):
    biospecimen_analysis = context['ti'].xcom_pull(task_ids='task_biospecimen_analysis_load', key='biospecimen_analysis')
    biospecimen_analysis['RUNDATE'] = pd.to_datetime(biospecimen_analysis['RUNDATE'], format='%Y-%m-%d', errors='coerce')
    columns_to_drop = ['SEX','COHORT','TYPE','InstrumentRep2','InstrumentRep3','PROJECTID','PI_NAME','PI_INSTITUTION']
    biospecimen_analysis.drop(columns=columns_to_drop, inplace=True)
    context['ti'].xcom_push(key='cleaned_biospecimen_analysis', value=biospecimen_analysis)
    return biospecimen_analysis

def filter_biospecimen_analysis(**context):
    biospecimen_analysis = context['ti'].xcom_pull(task_ids='task_clean_biospecimen_analysis', key='cleaned_biospecimen_analysis')
    earliest_records = (
        biospecimen_analysis[biospecimen_analysis['CLINICAL_EVENT'] == 'BL']
        .groupby('PATNO', as_index=False)
        .agg(earliest_date=('RUNDATE', 'min'))
    )
    biospecimen_analysis_cleaned = pd.merge(
        biospecimen_analysis,
        earliest_records,
        left_on=['PATNO', 'RUNDATE'],
        right_on=['PATNO', 'earliest_date'],
        how='inner'
    )
    context['ti'].xcom_push(key='filtered_biospecimen_analysis', value=biospecimen_analysis_cleaned)
    return biospecimen_analysis_cleaned

def clean_filtered_biospecimen_analysis(**context):
    biospecimen_analysis_cleaned = context['ti'].xcom_pull(task_ids='task_filter_biospecimen_analysis', key='filtered_biospecimen_analysis')
    columns_to_drop = ['CLINICAL_EVENT','RUNDATE','earliest_date']
    biospecimen_analysis_cleaned.drop(columns=columns_to_drop, inplace=True)
    context['ti'].xcom_push(key='cleaned_filtered_biospecimen_analysis', value=biospecimen_analysis_cleaned)
    return biospecimen_analysis_cleaned



def merge_biospecimen_with_participant(**context):
    biospecimen_analysis = context['ti'].xcom_pull(task_ids='task_clean_filtered_biospecimen_analysis', key='cleaned_filtered_biospecimen_analysis')
    combined_table = context['ti'].xcom_pull(task_ids='task_clean_participantstatus_demographic', key='cleaned_participantstatus_demographic')
    if biospecimen_analysis is None:
        raise ValueError("biospecimen_analysis is None. Check the task 'task_clean_filtered_biospecimen_analysis'.")
    if combined_table is None:
        raise ValueError("combined_table is None. Check the task 'task_clean_participantstatus_demographic'.")
    
    merged_data = pd.merge(
        combined_table,
        biospecimen_analysis,
        left_on='Participant_ID',
        right_on='PATNO',
        how='left'
    )
    context['ti'].xcom_push(key='merged_data', value=merged_data)
    return merged_data

def clean_participantstatus_demographics_biospecimen_analysis(**context):
    merged_data = context['ti'].xcom_pull(task_ids='task_merge_participantstatus_demographics_biospecimen_analysis', key='merged_data')
    merged_data.drop(columns=['PATNO'], inplace=True)
    
    return merged_data

# Task 0: Load participant status
task_participant_status_load = PythonOperator(
    task_id='task_participant_status_load',
    python_callable=participant_status_load,
    provide_context=True,
    dag=dag,
)

# Task 1: Load demographics
task_demographics_load = PythonOperator(
    task_id='task_demographics_load',
    python_callable=demographics_load,
    provide_context=True,
    dag=dag,
)

# Task 2: Clean participant status
task_clean_participant_status = PythonOperator(
    task_id='task_clean_participant_status',
    python_callable=clean_participant_status,
    provide_context=True,
    dag=dag,
)

# Task 3: Clean demographics
task_clean_demographics = PythonOperator(
    task_id='task_clean_demographics',
    python_callable=clean_demographics,
    provide_context=True,
    dag=dag,
)

# Task 4: Merge participant status and demographics
task_merge_participant_status_and_demographics = PythonOperator(
    task_id='task_merge_participant_status_and_demographics',
    python_callable=merge_participant_status_and_demographics,
    provide_context=True,
    dag=dag,
)

# Task 5: Clean participant status and demographics merged table
task_clean_participantstatus_demographic = PythonOperator(
    task_id='task_clean_participantstatus_demographic',
    python_callable=clean_participantstatus_demographic,
    provide_context=True,
    dag=dag,
)

# Task 6: Load Biospecimen Analysis Data
task_biospecimen_analysis_load = PythonOperator(
    task_id='task_biospecimen_analysis_load',
    python_callable=biospecimen_analysis_load,
    provide_context=True,
    dag=dag
)

# Task 7: Clean Biospecimen Analysis Data
task_clean_biospecimen_analysis = PythonOperator(
    task_id='task_clean_biospecimen_analysis',
    python_callable=clean_biospecimen_analysis,
    provide_context=True,
    dag=dag
)

# Task 8: Filter Biospecimen Analysis Data
task_filter_biospecimen_analysis = PythonOperator(
    task_id='task_filter_biospecimen_analysis',
    python_callable=filter_biospecimen_analysis,
    provide_context=True,
    dag=dag
)

# Task 9: Clean Filtered Biospecimen Analysis Data
task_clean_filtered_biospecimen_analysis = PythonOperator(
    task_id='task_clean_filtered_biospecimen_analysis',
    python_callable=clean_filtered_biospecimen_analysis,
    provide_context=True,
    dag=dag
)


# Task 10: Merge biospecimen analysis with participant status and demographics
task_merge_participantstatus_demographics_biospecimen_analysis = PythonOperator(
    task_id='task_merge_participantstatus_demographics_biospecimen_analysis',
    python_callable=merge_biospecimen_with_participant,
    provide_context=True,
    dag=dag,
)

# Task 11: Final clean-up task
task_clean_participantstatus_demographics_biospecimen_analysis = PythonOperator(
    task_id='task_clean_participantstatus_demographics_biospecimen_analysis',
    python_callable=clean_participantstatus_demographics_biospecimen_analysis,
    provide_context=True,
    dag=dag,
)

# Task 12: Send alert email in case of failure
task_send_alert_email = PythonOperator(
    task_id='task_send_alert_email',
    python_callable=send_custom_alert_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Setting the task dependencies
task_participant_status_load >> task_clean_participant_status
task_demographics_load >> task_clean_demographics
[task_clean_participant_status, task_clean_demographics] >> task_merge_participant_status_and_demographics >> task_clean_participantstatus_demographic

task_biospecimen_analysis_load >> task_clean_biospecimen_analysis >> task_filter_biospecimen_analysis >> task_clean_filtered_biospecimen_analysis

[task_clean_filtered_biospecimen_analysis, task_clean_participantstatus_demographic] >> task_merge_participantstatus_demographics_biospecimen_analysis >> task_clean_participantstatus_demographics_biospecimen_analysis >> task_send_alert_email
