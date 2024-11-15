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
import pickle


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
    #df=pd.read_csv('/home/mrudula/MLPOPS/outputs/airflow_cleaned_data.csv')
    context['ti'].xcom_push(key='df', value=df)
    return df
    
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
   

def select_best_model(**context):
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
    best_model_name, (best_model, best_accuracy) = max(model_accuracies.items(), key=lambda item: item[1][1])

    # Log and push the best model and accuracy
    context['ti'].xcom_push(key='best_model', value=best_model)
    context['ti'].xcom_push(key='best_model_name', value=best_model_name)
    context['ti'].xcom_push(key='best_accuracy', value=best_accuracy)
    
    print(f"Best Model: {best_model_name}, Validation Accuracy: {best_accuracy}")
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
# Send alert email in case of failure
task_send_alert_email = PythonOperator(
    task_id='task_send_alert_email',
    python_callable=send_custom_alert_email,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

get_data_from_data_pipeline_task>>split_to_X_y_task>>train_test_split_task>>[tune_LR_task,tune_RF_task,tune_SVM_task,tune_XGB_task]>>select_best_model_task>>test_best_model_task>>task_send_alert_email


