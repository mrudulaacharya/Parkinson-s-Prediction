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
# Directory where CSV files are stored
csv_directory = '/home/mrudula/MLPOPS/motor_senses/'

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
    context['ti'].xcom_push(key='merged_data_cleaned', value=merged_data)
    return merged_data


# Load functions for each CSV file
def load_motor_senses_1(**context):
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_I_27Oct2024.csv'))

def load_motor_senses_2(**context):
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_I_Patient_Questionnaire_27Oct2024.csv'))

def load_motor_senses_3(**context):
    return pd.read_csv(os.path.join(csv_directory, 'MDS_UPDRS_Part_II__Patient_Questionnaire_27Oct2024.csv'))

def load_motor_senses_4(**context):
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_III_27Oct2024.csv'))

def load_motor_senses_5(**context):
    return pd.read_csv(os.path.join(csv_directory, 'MDS-UPDRS_Part_IV__Motor_Complications_27Oct2024.csv'))
# Clean functions for each loaded CSV
def clean_motor_senses_1(**context):
    # Pull the DataFrame from XCom, making sure it's a DataFrame
    df = context['ti'].xcom_pull(task_ids='load_motor_senses_1_task')

    if isinstance(df, str):
        # If df is a string, it’s likely being serialized; read it as DataFrame
        df = pd.read_json(df)

    # Drop specified columns and return the cleaned DataFrame
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP1RTOT'])

def clean_motor_senses_2(**context):
    # Pull the DataFrame from XCom, making sure it's a DataFrame
    df = context['ti'].xcom_pull(task_ids='load_motor_senses_2_task')

    if isinstance(df, str):
        # If df is a string, it’s likely being serialized; read it as DataFrame
        df = pd.read_json(df)

    # Drop specified columns and return the cleaned DataFrame
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP1PTOT'])

def clean_motor_senses_3(**context):
    # Pull the DataFrame from XCom, making sure it's a DataFrame
    df = context['ti'].xcom_pull(task_ids='load_motor_senses_3_task')

    if isinstance(df, str):
        # If df is a string, it’s likely being serialized; read it as DataFrame
        df = pd.read_json(df)

    # Drop specified columns and return the cleaned DataFrame
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP2PTOT'])

def clean_motor_senses_4(**context):
    # Pull the DataFrame from XCom, making sure it's a DataFrame
    df = context['ti'].xcom_pull(task_ids='load_motor_senses_4_task')

    if isinstance(df, str):
        # If df is a string, it’s likely being serialized; read it as DataFrame
        df = pd.read_json(df)

    # Drop specified columns and return the cleaned DataFrame
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','PDTRTMNT','PDSTATE','HRPOSTMED','HRDBSON','HRDBSOFF','PDMEDYN','DBSYN','ONOFFORDER','OFFEXAM','OFFNORSN','DBSOFFTM','ONEXAM','ONNORSN','DBSONTM','PDMEDDT','PDMEDTM','EXAMDT','EXAMTM','NP3TOT'])

def clean_motor_senses_5(**context):
    # Pull the DataFrame from XCom, making sure it's a DataFrame
    df = context['ti'].xcom_pull(task_ids='load_motor_senses_5_task')

    if isinstance(df, str):
        # If df is a string, it’s likely being serialized; read it as DataFrame
        df = pd.read_json(df)

    # Drop specified columns and return the cleaned DataFrame
    return df.drop(columns=['REC_ID','PAG_NAME','INFODT','ORIG_ENTRY','LAST_UPDATE','NP4TOT'])

# Function to merge all cleaned CSVs
def merge_all_motor_senses_csvs(**context):
    # Pull cleaned DataFrames from XCom
    cleaned_dfs = [
        context['ti'].xcom_pull(task_ids='clean_motor_senses_1_task'),
        context['ti'].xcom_pull(task_ids='clean_motor_senses_2_task'),
        context['ti'].xcom_pull(task_ids='clean_motor_senses_3_task'),
        context['ti'].xcom_pull(task_ids='clean_motor_senses_4_task'),
        context['ti'].xcom_pull(task_ids='clean_motor_senses_5_task')
    ]
    
    # Merge all DataFrames (concatenation along rows)
    merged_df = pd.concat(cleaned_dfs, axis=0)
    
     # Push merged DataFrame to XCom
    context['ti'].xcom_push(key='merged_df', value=merged_df)
    print("Merged DataFrame pushed to XCom")

def drop_duplicate_motor_senses_columns(**context):
    # Retrieve merged DataFrame from XCom
    merged_df = context['ti'].xcom_pull(key='merged_df', task_ids='merge_all_motor_senses_csvs_task')
    
    # Drop duplicate columns
    deduped_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
    
    # Save the deduplicated DataFrame
    deduped_path = os.path.join(csv_directory, 'merged_deduped_file.csv')
    deduped_df.to_csv(deduped_path, index=False)
    context['ti'].xcom_push(key='deduped_df', value=deduped_df)
    
    print(f"Deduplicated merged file saved at {deduped_path}")


def load_and_merge_data(**context):
    participantstatus_demographics_biospecimen_merged_cleaned = context['ti'].xcom_pull(task_ids='task_clean_participantstatus_demographics_biospecimen_analysis', key='merged_data_cleaned')
    motor_senses_merged_cleaned= context['ti'].xcom_pull(task_ids='deduplication_motor_senses_task',key='deduped_df')

    # # Load the merged files from both tasks
    # df_participant = pd.read_csv(participant_path)
    # df_motor = pd.read_csv(motor_path)
    

    # Perform the merging operation
    merged_df_final = pd.merge(
        participantstatus_demographics_biospecimen_merged_cleaned,
        motor_senses_merged_cleaned,
        left_on="Participant_ID",
        right_on="PATNO",
        how='inner'
    )
    context['ti'].xcom_push(key='merged_final', value=merged_df_final)
    output_path = '/home/mrudula/MLPOPS/merged_data/merged_data_output.csv'
    
    return merged_df_final
load_and_merge_data
# Task 3: Data cleaning, preprocessing, and EDA
def clean_preprocess_eda(**context):
    data_final= context['ti'].xcom_pull(task_ids='load_and_merge_data',key='merged_final')     

    # Data Cleaning
    data_final = data_final.drop_duplicates()
    drop_threshold = 0.4 * len(data_final)
    missing_values = data_final.isnull().sum()
    columns_to_drop = missing_values[missing_values > drop_threshold].index.tolist()
    data_final = data_final.drop(columns=columns_to_drop)
    for col in data_final.columns:
        if data_final[col].dtype in ['float64', 'int64']:
            data_final[col].fillna(int(data_final[col].mean()), inplace=True)
        elif data_final[col].dtype == 'object':
            data_final[col].fillna(data_final[col].mode()[0], inplace=True)
    data_final.to_csv('/home/mrudula/MLPOPS/cleaned_data.csv', index=False)    

    # Check for irregularities
    if data_final.isnull().any().any():
        raise ValueError("Data irregularities detected: missing values present")
    return data_final

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
# Load tasks
load_motor_senses_1_task = PythonOperator(
    task_id='load_motor_senses_1_task',
    python_callable=load_motor_senses_1,
    provide_context=True,
    dag=dag,
)

load_motor_senses_2_task = PythonOperator(
    task_id='load_motor_senses_2_task',
    python_callable=load_motor_senses_2,
    provide_context=True,
    dag=dag,
)

load_motor_senses_3_task = PythonOperator(
    task_id='load_motor_senses_3_task',
    python_callable=load_motor_senses_3,
    provide_context=True,
    dag=dag,
)

load_motor_senses_4_task = PythonOperator(
    task_id='load_motor_senses_4_task',
    python_callable=load_motor_senses_4,
    provide_context=True,
    dag=dag,
)

load_motor_senses_5_task = PythonOperator(
    task_id='load_motor_senses_5_task',
    python_callable=load_motor_senses_5,
    provide_context=True,
    dag=dag,
)

# Clean tasks
clean_motor_senses_1_task = PythonOperator(
    task_id='clean_motor_senses_1_task',
    python_callable=clean_motor_senses_1,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_2_task = PythonOperator(
    task_id='clean_motor_senses_2_task',
    python_callable=clean_motor_senses_2,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_3_task = PythonOperator(
    task_id='clean_motor_senses_3_task',
    python_callable=clean_motor_senses_3,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_4_task = PythonOperator(
    task_id='clean_motor_senses_4_task',
    python_callable=clean_motor_senses_4,
    provide_context=True,
    dag=dag,
)

clean_motor_senses_5_task = PythonOperator(
    task_id='clean_motor_senses_5_task',
    python_callable=clean_motor_senses_5,
    provide_context=True,
    dag=dag,
)

# Merge task
merge_all_motor_senses_csvs_task = PythonOperator(
    task_id='merge_all_motor_senses_csvs_task',
    python_callable=merge_all_motor_senses_csvs,
    provide_context=True,
    dag=dag,
)

# Deduplication task that pulls merged DataFrame from XCom
deduplication_motor_senses_task = PythonOperator(
    task_id='deduplication_motor_senses_task',
    python_callable=drop_duplicate_motor_senses_columns,
    provide_context=True,
    dag=dag,
)
load_and_merge_task = PythonOperator(
    task_id='load_and_merge_data',
    python_callable=load_and_merge_data,
    provide_context=True,
    dag=dag,
)
clean_preprocess_eda_task = PythonOperator(
    task_id='clean_preprocess_eda',
    python_callable=clean_preprocess_eda,
    provide_context=True,
    dag=dag,
)

# Setting the task dependencies
task_participant_status_load >> task_clean_participant_status
task_demographics_load >> task_clean_demographics
[task_clean_participant_status, task_clean_demographics] >> task_merge_participant_status_and_demographics >> task_clean_participantstatus_demographic

task_biospecimen_analysis_load >> task_clean_biospecimen_analysis >> task_filter_biospecimen_analysis >> task_clean_filtered_biospecimen_analysis

[task_clean_filtered_biospecimen_analysis, task_clean_participantstatus_demographic] >> task_merge_participantstatus_demographics_biospecimen_analysis >> task_clean_participantstatus_demographics_biospecimen_analysis 
load_motor_senses_1_task >> clean_motor_senses_1_task
load_motor_senses_2_task >> clean_motor_senses_2_task
load_motor_senses_3_task >> clean_motor_senses_3_task
load_motor_senses_4_task >> clean_motor_senses_4_task
load_motor_senses_5_task >> clean_motor_senses_5_task
[clean_motor_senses_1_task,clean_motor_senses_2_task,clean_motor_senses_3_task,clean_motor_senses_4_task,clean_motor_senses_5_task]>>merge_all_motor_senses_csvs_task >> deduplication_motor_senses_task
[task_clean_participantstatus_demographics_biospecimen_analysis ,deduplication_motor_senses_task]>>load_and_merge_task>>clean_preprocess_eda_task>>task_send_alert_email
