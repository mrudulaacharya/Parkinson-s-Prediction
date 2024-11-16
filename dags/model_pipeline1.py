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
from sklearn.model_selection import train_test_split as sklearn_train_test_split
import xgboost as xgb
import mlflow
import mlflow.sklearn
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import GridSearchCV
import joblib
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from fairlearn.metrics import MetricFrame, demographic_parity_difference, equalized_odds_difference


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
    df = context['dag_run'].conf['processed_data']

    #df_unprocessed = context['dag_run'].conf['unprocessed_data']
    #df=pd.read_csv('/home/mrudula/MLPOPS/outputs/airflow_cleaned_data.csv')
    context['ti'].xcom_push(key='df', value=df)
    #context['ti'].xcom_push(key='df_unprocessed', value=df_unprocessed)
    return df

def validate_data(**context):

    df=context['ti'].xcom_pull(key='df', task_ids='get_data_from_data_pipeline')
    
    df1 = pd.read_json(df, orient='split')
    # 1. Shape and Size Check
    if df1.shape[0] == 0 or df1.shape[1] == 0:
        raise ValueError("DataFrame is empty!")

    # 2. Column Existence Check
    expected_columns = ['ENROLL_AGE', 'ENRLPRKN_0.0', 'ENRLPRKN_0.2', 'ENRLPRKN_1.0', 'ENRLSRDC_0.0', 'ENRLSRDC_0.2', 'ENRLSRDC_0.4', 'ENRLSRDC_0.6', 'ENRLSRDC_0.8', 'ENRLSRDC_1.0', 'ENRLHPSM_0.0', 'ENRLHPSM_1.0', 'ENRLRBD_0.0', 'ENRLRBD_1.0', 'ENRLSNCA_0.0', 'ENRLSNCA_1.0', 'ENRLGBA_0.0', 'ENRLGBA_1.0', 'SEX_0.0', 'SEX_1.0', 'NUPSOURC_1.0', 'NUPSOURC_2.0', 'NUPSOURC_3.0', 'NP1COG_0.0', 'NP1COG_1.0', 'NP1COG_2.0', 'NP1COG_3.0', 'NP1COG_4.0', 'NP1HALL_0.0', 'NP1HALL_1.0', 'NP1HALL_2.0', 'NP1HALL_3.0', 'NP1DPRS_0.0', 'NP1DPRS_1.0', 'NP1DPRS_2.0', 'NP1DPRS_3.0', 'NP1DPRS_4.0', 'NP1ANXS_0.0', 'NP1ANXS_1.0', 'NP1ANXS_2.0', 'NP1ANXS_3.0', 'NP1ANXS_4.0', 'NP1APAT_0.0', 'NP1APAT_1.0', 'NP1APAT_2.0', 'NP1APAT_3.0', 'NP1APAT_4.0', 'NP1DDS_0.0', 'NP1DDS_1.0', 'NP1DDS_2.0', 'NP1DDS_3.0', 'NP1DDS_101.0', 'NP1SLPN_0.0', 'NP1SLPN_1.0', 'NP1SLPN_2.0', 'NP1SLPN_3.0', 'NP1SLPN_4.0', 'NP1SLPD_0.0', 'NP1SLPD_1.0', 'NP1SLPD_2.0', 'NP1SLPD_3.0', 'NP1SLPD_4.0', 'NP1PAIN_0.0', 'NP1PAIN_1.0', 'NP1PAIN_2.0', 'NP1PAIN_3.0', 'NP1PAIN_4.0', 'NP1URIN_0.0', 'NP1URIN_1.0', 'NP1URIN_2.0', 'NP1URIN_3.0', 'NP1URIN_4.0', 'NP1CNST_0.0', 'NP1CNST_1.0', 'NP1CNST_2.0', 'NP1CNST_3.0', 'NP1CNST_4.0', 'NP1LTHD_0.0', 'NP1LTHD_1.0', 'NP1LTHD_2.0', 'NP1LTHD_3.0', 'NP1LTHD_4.0', 'NP1FATG_0.0', 'NP1FATG_1.0', 'NP1FATG_2.0', 'NP1FATG_3.0', 'NP1FATG_4.0', 'NP2SPCH_0.0', 'NP2SPCH_1.0', 'NP2SPCH_2.0', 'NP2SPCH_3.0', 'NP2SPCH_4.0', 'NP2SALV_0.0', 'NP2SALV_1.0', 'NP2SALV_2.0', 'NP2SALV_3.0', 'NP2SALV_4.0', 'NP2SWAL_0.0', 'NP2SWAL_1.0', 'NP2SWAL_2.0', 'NP2SWAL_3.0', 'NP2EAT_0.0', 'NP2EAT_1.0', 'NP2EAT_2.0', 'NP2EAT_3.0', 'NP2EAT_4.0', 'NP2DRES_0.0', 'NP2DRES_1.0', 'NP2DRES_2.0', 'NP2DRES_3.0', 'NP2HYGN_0.0', 'NP2HYGN_1.0', 'NP2HYGN_2.0', 'NP2HYGN_3.0', 'NP2HYGN_4.0', 'NP2HWRT_0.0', 'NP2HWRT_1.0', 'NP2HWRT_2.0', 'NP2HWRT_3.0', 'NP2HWRT_4.0', 'NP2HOBB_0.0', 'NP2HOBB_1.0', 'NP2HOBB_2.0', 'NP2HOBB_3.0', 'NP2HOBB_4.0', 'NP2TURN_0.0', 'NP2TURN_1.0', 'NP2TURN_2.0', 'NP2TURN_3.0', 'NP2TURN_4.0', 'NP2TRMR_0.0', 'NP2TRMR_1.0', 'NP2TRMR_2.0', 'NP2TRMR_3.0', 'NP2TRMR_4.0', 'NP2RISE_0.0', 'NP2RISE_1.0', 'NP2RISE_2.0', 'NP2RISE_3.0', 'NP2RISE_4.0', 'NP2WALK_0.0', 'NP2WALK_1.0', 'NP2WALK_2.0', 'NP2WALK_3.0', 'NP2WALK_4.0', 'NP2FREZ_0.0', 'NP2FREZ_1.0', 'NP2FREZ_2.0', 'NP2FREZ_3.0', 'NP2FREZ_4.0', 'NP3SPCH_0.0', 'NP3SPCH_1.0', 'NP3SPCH_2.0', 'NP3SPCH_3.0', 'NP3SPCH_4.0', 'NP3FACXP_0.0', 'NP3FACXP_1.0', 'NP3FACXP_2.0', 'NP3FACXP_3.0', 'NP3FACXP_4.0', 'NP3RIGN_0.0', 'NP3RIGN_1.0', 'NP3RIGN_2.0', 'NP3RIGN_3.0', 'NP3RIGN_4.0', 'NP3RIGN_101.0', 'NP3RIGRU_0.0', 'NP3RIGRU_1.0', 'NP3RIGRU_2.0', 'NP3RIGRU_3.0', 'NP3RIGRU_101.0', 'NP3RIGLU_0.0', 'NP3RIGLU_1.0', 'NP3RIGLU_2.0', 'NP3RIGLU_3.0', 'NP3RIGLU_4.0', 'NP3RIGLU_101.0', 'NP3RIGRL_0.0', 'NP3RIGRL_1.0', 'NP3RIGRL_2.0', 'NP3RIGRL_3.0', 'NP3RIGRL_101.0', 'NP3RIGLL_0.0', 'NP3RIGLL_1.0', 'NP3RIGLL_2.0', 'NP3RIGLL_3.0', 'NP3RIGLL_4.0', 'NP3RIGLL_101.0', 'NP3FTAPR_0.0', 'NP3FTAPR_1.0', 'NP3FTAPR_2.0', 'NP3FTAPR_3.0', 'NP3FTAPR_4.0', 'NP3FTAPR_101.0', 'NP3FTAPL_0.0', 'NP3FTAPL_1.0', 'NP3FTAPL_2.0', 'NP3FTAPL_3.0', 'NP3FTAPL_4.0', 'NP3FTAPL_101.0', 'NP3HMOVR_0.0', 'NP3HMOVR_1.0', 'NP3HMOVR_2.0', 'NP3HMOVR_3.0', 'NP3HMOVR_4.0', 'NP3HMOVR_101.0', 'NP3HMOVL_0.0', 'NP3HMOVL_1.0', 'NP3HMOVL_2.0', 'NP3HMOVL_3.0', 'NP3HMOVL_101.0', 'NP3PRSPR_0.0', 'NP3PRSPR_1.0', 'NP3PRSPR_2.0', 'NP3PRSPR_3.0', 'NP3PRSPL_0.0', 'NP3PRSPL_1.0', 'NP3PRSPL_2.0', 'NP3PRSPL_3.0', 'NP3PRSPL_4.0', 'NP3PRSPL_101.0', 'NP3TTAPR_0.0', 'NP3TTAPR_1.0', 'NP3TTAPR_2.0', 'NP3TTAPR_3.0', 'NP3TTAPR_4.0', 'NP3TTAPR_101.0', 'NP3TTAPL_0.0', 'NP3TTAPL_1.0', 'NP3TTAPL_2.0', 'NP3TTAPL_3.0', 'NP3TTAPL_4.0', 'NP3TTAPL_101.0', 'NP3LGAGR_0.0', 'NP3LGAGR_1.0', 'NP3LGAGR_2.0', 'NP3LGAGR_3.0', 'NP3LGAGR_101.0', 'NP3LGAGL_0.0', 'NP3LGAGL_1.0', 'NP3LGAGL_2.0', 'NP3LGAGL_3.0', 'NP3LGAGL_4.0', 'NP3LGAGL_101.0', 'NP3RISNG_0.0', 'NP3RISNG_1.0', 'NP3RISNG_2.0', 'NP3RISNG_3.0', 'NP3RISNG_101.0', 'NP3GAIT_0.0', 'NP3GAIT_1.0', 'NP3GAIT_2.0', 'NP3GAIT_3.0', 'NP3GAIT_4.0', 'NP3GAIT_101.0', 'NP3FRZGT_0.0', 'NP3FRZGT_1.0', 'NP3FRZGT_2.0', 'NP3FRZGT_3.0', 'NP3FRZGT_4.0', 'NP3FRZGT_101.0', 'NP3PSTBL_0.0', 'NP3PSTBL_1.0', 'NP3PSTBL_2.0', 'NP3PSTBL_3.0', 'NP3PSTBL_4.0', 'NP3PSTBL_101.0', 'NP3POSTR_0.0', 'NP3POSTR_1.0', 'NP3POSTR_2.0', 'NP3POSTR_3.0', 'NP3POSTR_4.0', 'NP3POSTR_101.0', 'NP3BRADY_0.0', 'NP3BRADY_1.0', 'NP3BRADY_2.0', 'NP3BRADY_3.0', 'NP3PTRMR_0.0', 'NP3PTRMR_1.0', 'NP3PTRMR_2.0', 'NP3PTRMR_3.0', 'NP3PTRMR_101.0', 'NP3PTRML_0.0', 'NP3PTRML_1.0', 'NP3PTRML_2.0', 'NP3PTRML_3.0', 'NP3KTRMR_0.0', 'NP3KTRMR_1.0', 'NP3KTRMR_2.0', 'NP3KTRMR_3.0', 'NP3KTRMR_101.0', 'NP3KTRML_0.0', 'NP3KTRML_1.0', 'NP3KTRML_2.0', 'NP3KTRML_3.0', 'NP3KTRML_101.0', 'NP3RTARU_0.0', 'NP3RTARU_1.0', 'NP3RTARU_2.0', 'NP3RTARU_3.0', 'NP3RTARU_4.0', 'NP3RTALU_0.0', 'NP3RTALU_1.0', 'NP3RTALU_2.0', 'NP3RTALU_3.0', 'NP3RTALU_4.0', 'NP3RTARL_0.0', 'NP3RTARL_1.0', 'NP3RTARL_2.0', 'NP3RTARL_3.0', 'NP3RTARL_4.0', 'NP3RTALL_0.0', 'NP3RTALL_1.0', 'NP3RTALL_2.0', 'NP3RTALL_3.0', 'NP3RTALJ_0.0', 'NP3RTALJ_1.0', 'NP3RTALJ_2.0', 'NP3RTALJ_4.0', 'NP3RTALJ_101.0', 'NP3RTCON_0.0', 'NP3RTCON_1.0', 'NP3RTCON_2.0', 'NP3RTCON_3.0', 'NP3RTCON_4.0', 'NP3RTCON_101.0', 'DYSKPRES_0.0', 'DYSKPRES_1.0', 'NHY_0.0', 'NHY_1.0', 'NHY_2.0', 'NHY_3.0', 'NHY_4.0', 'NHY_101.0', 'COHORT']  # Update with your actual expected columns
    if not all(col in df1.columns for col in expected_columns):
        raise ValueError(f"Missing columns: {set(expected_columns) - set(df1.columns)}")

    # 3. Null Values Check
    if df1.isnull().any().any():
        raise ValueError("Data contains missing values!")
    #context['ti'].xcom_push(key='df', value=df)

    return True
    
def split_to_X_y(**context):
    
    df=context['ti'].xcom_pull(key='df', task_ids='get_data_from_data_pipeline')
    df = pd.read_json(df, orient='split')
    
    # if isinstance(df, str):
    #     # If df is a string, it’s likely being serialized; read it as DataFrame
    #     df = pd.read_json(df ,orient='split')
    logging.info(f"Retrieved df from XCom: {df}")
    y=df['COHORT']    
    y = LabelEncoder().fit_transform(y)
    X=df.drop(columns=['COHORT'])
    context['ti'].xcom_push(key='y', value=y.tolist())
    context['ti'].xcom_push(key='X', value=X)
    return X,y.tolist()

def train_test_split(**context):

    X= context['ti'].xcom_pull(key='X', task_ids='split_to_X_y')
    y= context['ti'].xcom_pull(key='y', task_ids='split_to_X_y')
    # Split data into train/test
    X_train, X_temp, y_train, y_temp = sklearn_train_test_split(X, y, test_size=0.30, random_state=42)
    X_val, X_test, y_val, y_test = sklearn_train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)
    context['ti'].xcom_push(key='y_train', value=y_train)
    context['ti'].xcom_push(key='X_train', value=X_train)
    context['ti'].xcom_push(key='y_val', value=y_val)
    context['ti'].xcom_push(key='X_val', value=X_val)
    context['ti'].xcom_push(key='y_test', value=y_test)
    context['ti'].xcom_push(key='X_test', value=X_test)
    return X_train,y_train,X_val,y_val,X_test,y_test



def train_and_tune_model(model, param_grid, model_name, X_train, y_train, X_val, y_val, **context):
    with mlflow.start_run(run_name=f"{model_name}_Tuning"):
        grid_search = GridSearchCV(model, param_grid=param_grid, cv=3, scoring='accuracy', n_jobs=-1)
        grid_search.fit(X_train, y_train)
        
        best_model = grid_search.best_estimator_
        best_params = grid_search.best_params_
        val_accuracy = best_model.score(X_val, y_val)
        
        # Log best parameters and metrics
        mlflow.log_params(best_params)
        mlflow.log_metric("validation_accuracy", val_accuracy)
        mlflow.sklearn.log_model(best_model, f"{model_name}_Tuned_Model")
        
        print(f"{model_name} Best Params: {best_params}, Validation Accuracy: {val_accuracy}")
        
        # Push best model to XCom
        #context['ti'].xcom_push(key=f"best_{model_name.lower()}_model", value=best_model)
        
        return best_model
    
def evaluate_on_test_set(model, X_test, y_test):
    with mlflow.start_run(run_name=f"Test_{model.__class__.__name__}"):
        y_pred = model.predict(X_test)
        
        # Calculate test accuracy and log it
        test_accuracy = accuracy_score(y_test, y_pred)
        mlflow.log_metric("test_accuracy", test_accuracy)
        
        # Print and log classification report
        report = classification_report(y_test, y_pred, output_dict=True)
        mlflow.log_metrics({"accuracy": report["accuracy"]}, step=1)

        
        # Log classification report as text for easy access
        report_text = classification_report(y_test, y_pred)
        print(f"Test Accuracy for {model.__class__.__name__}: {test_accuracy}")
        print(f"Classification Report:\n{report_text}")
        
        return test_accuracy, report_text
    


# Define functions that wrap the training and tuning function for each model
def tune_SVM(**context):
    param_grid_svm = {
        'C': [0.1, 1, 10],
        'kernel': ['linear', 'rbf'],
        'gamma': ['scale', 'auto']
    }
    svm_model = SVC(random_state=42)
    best_model_SVM= train_and_tune_model(svm_model, param_grid_svm, "SVM", 
                                context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                **context)
    svm_model_filename = f"/home/mrudula/MLPOPS/models/best_svm_model.pkl"
    joblib.dump(best_model_SVM, svm_model_filename)
    context['ti'].xcom_push(key=f"best_svm_model", value=svm_model_filename)

    
def tune_RF(**context):
    param_grid_rf = {
        'n_estimators': [50, 100, 200],
        'max_depth': [10, 20, 30],
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2]
    }
    rf_model = RandomForestClassifier(random_state=42)
    best_model_rf= train_and_tune_model(rf_model, param_grid_rf, "RF", 
                                context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                **context)
    rf_model_filename = f"/home/mrudula/MLPOPS/models/best_rf_model.pkl"
    joblib.dump(best_model_rf, rf_model_filename)
    context['ti'].xcom_push(key=f"best_rf_model", value=rf_model_filename)
    
def tune_XGB(**context):
    param_grid_xgb = {
        'max_depth': [3, 6, 10],
        'learning_rate': [0.01, 0.1, 0.3],
        'n_estimators': [50, 100, 200],
        'subsample': [0.8, 0.9, 1.0]
    }
    xgb_model = xgb.XGBClassifier(random_state=42)
    best_model_xgb= train_and_tune_model(xgb_model, param_grid_xgb, "XGB", 
                                context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                **context)
    
    xgb_model_filename = f"/home/mrudula/MLPOPS/models/best_xgb_model.pkl"
    joblib.dump(best_model_xgb, xgb_model_filename)
    context['ti'].xcom_push(key=f"best_xgb_model", value=xgb_model_filename)
    
def tune_LR(**context):
    param_grid_lr = {
        'C': [0.1, 1, 10],
        'solver': ['liblinear', 'saga'],
        'penalty': ['l2']
    }
    lr_model = LogisticRegression(random_state=42)
    best_model_lr= train_and_tune_model(lr_model, param_grid_lr, "LR", 
                                context['ti'].xcom_pull(key='X_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_train', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'), 
                                context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'), 
                                **context)
    lr_model_filename = f"/home/mrudula/MLPOPS/models/best_lr_model.pkl"
    joblib.dump(best_model_lr, lr_model_filename)
    context['ti'].xcom_push(key=f"best_lr_model", value=lr_model_filename)


def model_accuracies(**context):
    # Retrieve validation accuracies and models from XCom
    svm_model = context['ti'].xcom_pull(key='best_svm_model', task_ids='tune_SVM')
    svm=joblib.load(svm_model)

    rf_model = context['ti'].xcom_pull(key='best_rf_model', task_ids='tune_RF')
    rf=joblib.load(rf_model)
    xgb_model = context['ti'].xcom_pull(key='best_xgb_model', task_ids='tune_XGB')
    xgb=joblib.load(xgb_model)
    lr_model = context['ti'].xcom_pull(key='best_lr_model', task_ids='tune_LR')
    lr=joblib.load(lr_model)
    # Retrieve validation accuracies
    svm_accuracy = svm.score(context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'),
                                   context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'))
    rf_accuracy = rf.score(context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'),
                                 context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'))
    xgb_accuracy = xgb.score(context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'),
                                   context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'))
    lr_accuracy = lr.score(context['ti'].xcom_pull(key='X_val', task_ids='train_test_split'),
                                 context['ti'].xcom_pull(key='y_val', task_ids='train_test_split'))

    # Store models and accuracies in a dictionary
    model_accuracies = {
        'SVM': (svm_model, svm_accuracy),
        'Random Forest': (rf_model, rf_accuracy),
        'XGBoost': (xgb_model, xgb_accuracy),
        'Logistic Regression': (lr_model, lr_accuracy)
    }

    # Find the model with the highest accuracy
    #best_model_name, (best_model, best_accuracy) = max(model_accuracies.items(), key=lambda item: item[1][1])

    # # Log and push the best model and accuracy
    # context['ti'].xcom_push(key='best_model', value=best_model)
    # context['ti'].xcom_push(key='best_model_name', value=best_model_name)
    # context['ti'].xcom_push(key='best_accuracy', value=best_accuracy)
    
    #print(f"Best Model: {best_model_name}, Validation Accuracy: {best_accuracy}")
    context['ti'].xcom_push(key='model_accuracies', value=model_accuracies)
    return model_accuracies



def bias_report(**context):
    """
    Compute fairness metrics for multiple models and log results using MLflow.

    Parameters:
    - X_val: DataFrame, validation feature set (one-hot encoded for categorical variables like SEX).
    - y_val: Series or ndarray, validation target set.
    - models: Dictionary of {model_name: model_object}.
    - sensitive_feature_columns: List of columns in X_val used as sensitive features for fairness evaluation.
    - mlflow_logger: MLflow logging object.
    
    Returns:
    - fairness_report: Dictionary containing fairness metrics for all models.

    """
    # Retrieve models from XCom
    models = {
        'SVM': context['ti'].xcom_pull(key='best_svm_model', task_ids='tune_SVM'),
        'Random Forest': context['ti'].xcom_pull(key='best_rf_model', task_ids='tune_RF'),
        'XGBoost': context['ti'].xcom_pull(key='best_xgb_model', task_ids='tune_XGB'),
        'Logistic Regression': context['ti'].xcom_pull(key='best_lr_model', task_ids='tune_LR'),
    }

    # Load models
    loaded_models = {name: joblib.load(model) for name, model in models.items()}

    # Retrieve validation data and labels
    X_val = context['ti'].xcom_pull(key='X_val', task_ids='train_test_split')
    y_val = context['ti'].xcom_pull(key='y_val', task_ids='train_test_split')
    y_val = pd.Series(y_val, index=X_val.index)
    
    # Initialize a list to hold the results
    model_results = []
    
    # Define sensitive feature columns
    sensitive_feature_columns = ['SEX_0.0', 'SEX_1.0', 'ENROLL_AGE']
    sensitive_features = X_val[sensitive_feature_columns].copy()

    # Handle ENROLL_AGE_BINNED creation
    max_value = X_val["ENROLL_AGE"].max()
    min_value = X_val["ENROLL_AGE"].min()
    num_bins = 4
    sensitive_features["ENROLL_AGE_BINNED"] = pd.cut(
        X_val["ENROLL_AGE"],
        bins=np.linspace(min_value, max_value, num_bins + 1),
        labels=[f"Bin {i}" for i in range(1, num_bins + 1)],
        right=False
    ).astype(str)

    # Evaluate fairness for each model
    for model_name, model in loaded_models.items():
        print(f"Evaluating fairness for model: {model_name}")
        mlflow.start_run(run_name=f"Fairness Evaluation - {model_name}")
        
        # Predict on validation data
        y_pred = model.predict(X_val)

        for class_label in np.unique(y_val):
            print(f"Analyzing fairness for class: {class_label}")
            
            # Binarize the y_val and y_pred for the current class
            y_val_binary = (y_val == class_label).astype(int)
            y_pred_binary = (y_pred == class_label).astype(int)

        # Loop through sensitive features and calculate fairness metrics
            for feature in sensitive_features.columns:
                print(f"Analyzing fairness for sensitive feature: {feature}")
                
                # Get the sensitive feature values for the current feature
                feature_sensitive_values = sensitive_features[feature]

                # Demographic Parity Difference
                dp_diff = demographic_parity_difference(
                    y_val_binary,
                    y_pred_binary,
                    sensitive_features=feature_sensitive_values
                )

                # Equalized Odds Difference (if required)
                eo_diff = equalized_odds_difference(
                    y_val_binary,
                    y_pred_binary,
                    sensitive_features=feature_sensitive_values
                )
                accuracy = (y_pred_binary == y_val_binary).mean()


                # Append results for each bin
                if feature == 'ENROLL_AGE_BINNED':
                    for age_bin in sensitive_features[feature].unique():
                        accuracy = (y_pred == y_val).mean()  # Calculate accuracy
                        model_results.append({
                            'model': model_name,
                            'slice': f"AGE_{age_bin}",
                            'accuracy': accuracy,
                            'dp_diff': dp_diff,
                            'eo_diff': eo_diff
                        })
                else:
                    # For non-age features, calculate accuracy and log fairness metrics
                    accuracy = (y_pred == y_val).mean()  # Calculate accuracy
                    model_results.append({
                        'model': model_name,
                        'slice': f"{feature}",
                        'accuracy': accuracy,
                        'dp_diff': dp_diff,
                        'eo_diff': eo_diff
                    })

        mlflow.end_run()

    # Log the model results
    context['ti'].xcom_push(key='bias_report', value=model_results)

    return model_results




def model_ranking(**context):
    # Pull fairness report and validation data
    fairness_report = context['ti'].xcom_pull(key='bias_report', task_ids='bias_report')
    model_accuracies = context['ti'].xcom_pull(key='model_accuracies', task_ids='model_accuracies')
    
    model_fairness_penalties = {}

    # Loop through the fairness report to calculate bias penalties
    for entry in fairness_report:
        model_name = entry['model']
        dp_diff = entry['dp_diff']
        eo_diff = entry['eo_diff']
        
        # Initialize the penalty for the model if not already done
        if model_name not in model_fairness_penalties:
            model_fairness_penalties[model_name] = []

        # Append the absolute value of dp_diff and eo_diff (larger is worse)
        model_fairness_penalties[model_name].append(abs(dp_diff) + abs(eo_diff))
    
    # Step 2: Calculate the average penalty for each model
    for model_name in model_fairness_penalties:
        penalties = model_fairness_penalties[model_name]
        average_penalty = np.mean(penalties)  # Average of all sensitive features for this model
        model_fairness_penalties[model_name] = average_penalty
    
    # Step 3: Rank models based on both accuracy and fairness
    model_scores = []
    
    for model_name, (model, accuracy) in model_accuracies.items():
        fairness_penalty = model_fairness_penalties.get(model_name, 0)
        # Calculate a score considering both accuracy and fairness penalty
        score = accuracy - 0.7 * fairness_penalty
        model_scores.append((model_name, model,score))
    
    
    # Step 4: Sort models based on the score (higher is better)
    ranked_models = [(model_name,model,score) for model_name,model, score in sorted(model_scores, key=lambda x: x[1], reverse=True)]
    
    context['ti'].xcom_push(key='model_ranking', value=ranked_models)
    return ranked_models


def select_best_model(**context):

    model_ranking_df = context['ti'].xcom_pull(key='model_ranking', task_ids='model_ranking')
    
    if  not model_ranking_df :
        raise ValueError("Model ranking DataFrame not found or is empty in XCom.")
    
    best_model_name, best_model,score = model_ranking_df[0]
    print("Best model:", best_model_name)
    
    # Push the best model's details to XCom
    context['ti'].xcom_push(key='best_model', value=best_model)   
    
    context['ti'].xcom_push(key='best_model_name', value=best_model_name)    
    
    return best_model


def test_best_model(**context):
    # Pull the best model from XCom
    best_model = context['ti'].xcom_pull(key="best_model",task_ids='select_best_model')
    model=joblib.load(best_model)

    X_test = context['ti'].xcom_pull(key='X_test', task_ids='train_test_split')
    y_test = context['ti'].xcom_pull(key='y_test', task_ids='train_test_split')

    # Evaluate test accuracy and classification report
    test_accuracy, report_text = evaluate_on_test_set(model, X_test, y_test)

    print(f"Test Accuracy for best model: {test_accuracy}")
    print(f"Classification Report:\n{report_text}")
    return test_accuracy, report_text

    
def register_best_model(**context):
     # Pull the best model and model name from XCom
    best_model = context['ti'].xcom_pull(key='best_model', task_ids='select_best_model')
    best_model_name = context['ti'].xcom_pull(key='best_model_name', task_ids='select_best_model')
    
    # Start MLflow run for model registration
    with mlflow.start_run(run_name="Model_Registration"):
        # Log the model
        model_path = f"models/{best_model_name}"
        mlflow.sklearn.log_model(best_model, model_path)
        
        # Register the model to the model registry
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/{model_path}"
        mlflow.register_model(model_uri, best_model_name)
        
    print(f"Registered {best_model_name} as the best model.")



get_data_from_data_pipeline_task = PythonOperator(
    task_id='get_data_from_data_pipeline',
    python_callable= get_data_from_data_pipeline,
    provide_context=True,
    dag=dag,
)
validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable= validate_data,
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

tune_SVM_task = PythonOperator(
    task_id='tune_SVM',
    python_callable=tune_SVM,
    provide_context=True,
    dag=dag,
)

tune_LR_task = PythonOperator(
    task_id='tune_LR',
    python_callable=tune_LR,
    provide_context=True,
    dag=dag,
)
tune_RF_task = PythonOperator(
    task_id='tune_RF',
    python_callable=tune_RF,
    provide_context=True,
    dag=dag,
)
tune_XGB_task = PythonOperator(
    task_id='tune_XGB',
    python_callable=tune_XGB,
    provide_context=True,
    dag=dag,
)

model_accuracies_task = PythonOperator(
    task_id='model_accuracies',
    python_callable=model_accuracies,
    provide_context=True,
    dag=dag,
)

bias_report_task = PythonOperator(
    task_id='bias_report',
    python_callable=bias_report,
    provide_context=True,
    dag=dag,
)

model_ranking_task = PythonOperator(
    task_id='model_ranking',
    python_callable=model_ranking,
    provide_context=True,
    dag=dag,
)


select_best_model_task = PythonOperator(
    task_id='select_best_model',
    python_callable=select_best_model,
    provide_context=True,
    dag=dag,
)
test_best_model_task = PythonOperator(
    task_id='test_best_model',
    python_callable=test_best_model,
    provide_context=True,
    dag=dag,
)

register_best_model_task=PythonOperator(
    task_id='register_best_model',
    python_callable=register_best_model,
    provide_context=True,
    dag=dag,
)

# Send alert email in case of failure
task_send_alert_email = PythonOperator(
    task_id='task_send_alert_email',
    python_callable=send_custom_alert_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

get_data_from_data_pipeline_task>>validate_data_task>>split_to_X_y_task>>train_test_split_task>>[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]
[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]>>model_accuracies_task
[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]>>bias_report_task
[bias_report_task,model_accuracies_task]>>model_ranking_task>>select_best_model_task>>test_best_model_task>>register_best_model_task>>task_send_alert_email