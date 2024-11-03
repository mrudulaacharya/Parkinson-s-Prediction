
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
# Define your function
#Task 0a
def participant_demographics_biospecimen_merged(**kwargs):
    # Load Participant_Status table
    #participant_status = pd.read_csv('/home/mrudula/MLPOPS/data_raw/Participant_Status_27Oct2024.csv')

    # Load Demographics table
    demographics = pd.read_csv('/home/mrudula/MLPOPS/data_raw/Demographics_27Oct2024.csv')

    # Renaming the PATNO column in Participant_Status for clarity
    participant_status.rename(columns={"PATNO": "Participant_ID"}, inplace=True)

    # Merging the DataFrames
    combined_table = pd.merge(
        participant_status,
        demographics,
        left_on="Participant_ID",
        right_on="PATNO",
        suffixes=("", "_drop")
    )

    # Dropping unnecessary columns
    columns_to_drop = [
        'HETERO', 'PANSEXUAL', 'ASEXUAL', 'OTHSEXUALITY', 
        'ORIG_ENTRY', 'LAST_UPDATE', 'HOWLIVE', 'GAYLES', 
        'PPMI_ONLINE_ENROLL', 'INEXPAGE', 'STATUS_DATE', 
        'COHORT_DEFINITION', 'REC_ID', 'PATNO'
    ]
    combined_table.drop(columns=columns_to_drop, inplace=True)

    # Filtering rows based on the Enrollment Status column
    valid_statuses = ['Enrolled', 'Complete', 'Withdrew']
    combined_table = combined_table[combined_table['ENROLL_STATUS'].isin(valid_statuses)]
    combined_table.drop(columns=['ENROLL_STATUS'], inplace=True)

    # Load Biospecimen_Analysis DataFrame
    biospecimen_analysis = pd.read_csv('/home/mrudula/MLPOPS/data_raw/SAA_Biospecimen_Analysis_Results_27Oct2024.csv')

    # Filter for clinical event 'BL' and find the earliest RUNDATE for each PATNO
    earliest_records = (
        biospecimen_analysis[biospecimen_analysis['CLINICAL_EVENT'] == 'BL']
        .groupby('PATNO', as_index=False)
        .agg(earliest_date=('RUNDATE', 'min'))
    )

    # Merge the original DataFrame with the earliest_records DataFrame
    biospecimen_analysis_cleaned = pd.merge(
        biospecimen_analysis,
        earliest_records,
        left_on=['PATNO', 'RUNDATE'],
        right_on=['PATNO', 'earliest_date'],
        how='inner'
    )

    # Drop specified columns from the Biospecimen_Analysis DataFrame
    columns_to_drop_biospecimen = ['SEX', 'COHORT', 'CLINICAL_EVENT', 'TYPE', 'PI_NAME', 'PI_INSTITUTION']
    biospecimen_analysis_cleaned.drop(columns=columns_to_drop_biospecimen, inplace=True)

    # Perform a left join between combined_table and Biospecimen_Analysis_Cleaned
    merged_table = pd.merge(
        combined_table,
        biospecimen_analysis_cleaned,
        left_on='Participant_ID',
        right_on='PATNO',
        how='left'
    )

    # You can choose to save the merged_table to a file, a database, or further process it
    # Example: saving to CSV
    #merged_table.to_csv('/home/mrudula/MLPOPS/cleaned_to_merge/merged_participant_biospecimen.csv', index=False)
    output_path = '/home/mrudula/MLPOPS/cleaned_to_merge/merged_participant_biospecimen.csv'
    merged_table.to_csv(output_path, index=False)
    return output_path

merge_patient_demographics_task = PythonOperator(
        task_id='participant_demographics_biospecimen_merged',
        python_callable=participant_demographics_biospecimen_merged,
        provide_context=True,
        dag=dag,
    )

    #Task 0b
def motor_merged(**kwargs):
    # Load the datasets
    file_1 = "/home/mrudula/MLPOPS/motor_senses/MDS_UPDRS_Part_II__Patient_Questionnaire_27Oct2024.csv"
    file_2 = "/home/mrudula/MLPOPS/motor_senses/MDS-UPDRS_Part_I_27Oct2024.csv"
    file_3 = "/home/mrudula/MLPOPS/motor_senses/MDS-UPDRS_Part_I_Patient_Questionnaire_27Oct2024.csv"
    file_4 = "/home/mrudula/MLPOPS/motor_senses/MDS-UPDRS_Part_III_27Oct2024.csv"
    file_5 = "/home/mrudula/MLPOPS/motor_senses/MDS-UPDRS_Part_IV__Motor_Complications_27Oct2024.csv"
    
    # Load each dataset into a DataFrame
    df1 = pd.read_csv(file_1)
    df2 = pd.read_csv(file_2)
    df3 = pd.read_csv(file_3)
    df4 = pd.read_csv(file_4)
    df5 = pd.read_csv(file_5)

    # Merge datasets with unique suffixes to avoid column conflicts
    merged_df = df1.merge(df2, on=['PATNO', 'EVENT_ID', 'INFODT'], how='outer', suffixes=('_part2', '_part1'))
    merged_df = merged_df.merge(df3, on=['PATNO', 'EVENT_ID', 'INFODT'], how='outer', suffixes=('', '_part1q'))
    merged_df = merged_df.merge(df4, on=['PATNO', 'EVENT_ID', 'INFODT'], how='outer', suffixes=('', '_part3'))
    merged_df = merged_df.merge(df5, on=['PATNO', 'EVENT_ID', 'INFODT'], how='outer', suffixes=('', '_part4'))

    # Drop irrelevant columns if they exist
    columns_to_drop = [
        'REC_ID_part2', 'REC_ID_part1', 'ORIG_ENTRY', 'LAST_UPDATE', 'PAG_NAME_part2', 'PAG_NAME_part1',
        'ORIG_ENTRY.1', 'LAST_UPDATE.1', 'ORIG_ENTRY.2', 'LAST_UPDATE.2', 'ORIG_ENTRY.3', 'LAST_UPDATE.3'
    ]
    merged_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

    # Convert date columns to datetime format
    merged_df['INFODT'] = pd.to_datetime(merged_df['INFODT'], errors='coerce')

    # Clean up column names for readability
    merged_df.columns = [col.replace('_x', '').replace('_y', '') for col in merged_df.columns]

    # Save the cleaned, merged data
    output_path = '/home/mrudula/MLPOPS/cleaned_to_merge/merged_mds_updrs.csv'
    merged_df.to_csv(output_path, index=False)
    
    return output_path

# Define the task in the DAG
motor_merged_task = PythonOperator(
    task_id='motor_merged',
    python_callable=motor_merged,
    provide_context=True,
    dag=dag,
)




# Task 1: Load and merge data
def load_and_merge_data(**kwargs):
    participant_path = kwargs['ti'].xcom_pull(task_ids='participant_demographics_biospecimen_merged')
    motor_path = kwargs['ti'].xcom_pull(task_ids='motor_merged')

    # Load the merged files from both tasks
    df_participant = pd.read_csv(participant_path)
    df_motor = pd.read_csv(motor_path)

    # Perform the merging operation
    merged_df = df_participant.merge(df_motor, on='PATNO', how='inner')

    output_path = '/home/mrudula/MLPOPS/merged_data/merged_data_output.csv'
    merged_df.to_csv(output_path, index=False)
    return output_path

load_and_merge_task = PythonOperator(
    task_id='load_and_merge_data',
    python_callable=load_and_merge_data,
    provide_context=True,
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
    df = df.drop(columns=columns_to_drop,errors='ignore')
    output_path = '/home/mrudula/MLPOPS/outputs/data_modified.csv'
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
[merge_patient_demographics_task, motor_merged_task]  >> load_and_merge_task >> drop_columns_task >> clean_preprocess_eda_task >> email_alert_task
