# Project Overview
This project aims to predict Parkinson's disease using machine learning algorithms and MLOPS techniques. By analyzing biomedical data such as demographic attributes, motor skills, and other relevant biomarkers this model can assist in early identification of Parkinson's disease symptoms. Parkinson's disease is a progressive neurological disorder with no known cure, but early detection can significantly improve patient outcomes by enabling earlier interventions. Predictive models help in identifying the disease at an early stage when treatments can be more effective in managing symptoms, thereby improving the quality of life for affected individuals. This project supports healthcare professionals by providing a tool for early detection, which can aid in timely diagnosis and treatment planning. Additionally, it can be a valuable resource for researchers studying Parkinson's disease, potentially contributing to the discovery of new biomarkers or insights into disease progression. 

## Environment Setup
python3 -m venv env
source env/bin/activate  # On Windows, use `env\Scripts\activate`

## Installing dependencies
pip install -r requirements.txt </br>
The requirements.txt includes essential packages like pandas, numpy, scikit-learn, tensorflow, matplotlib,airflow and others used in the pipeline.

### Prerequisites
To run this project, you need:
- Python 3.8 or above
- `pip` for package management

### Installation
1. **Clone the repository**:
   ```bash
   git clone https://github.com/mrudulaacharya/Parkinson-s-Prediction.git
   cd Parkinson-s-Prediction

## Dataset
The dataset is sourced from the Parkinson's Progression Markers Initiative (PPMI) and comprises comprehensive biomedical data, including demographic details, motor assessments, and various biomarkers pertinent to Parkinson's disease.

## Project Folder Structure

```plaintext
Parkinsons-Prediction/
├── README.md                     # Project documentation
├── data/                         # Folder for datasets 
├── scripts/                      # Scripts for data handling and preprocessing
│   ├── Correlation.ipynb         # Notebook for correlation analysis
│   ├── DataProfiling.ipynb       # Data profiling notebook
│   ├── bias_analysis.ipynb       # Notebook for bias analysis
│   ├── data_merging.ipynb        # Notebook for data merging
│   ├── data_schema.ipynb         # Notebook for defining data schema
│   ├── merge_data.py             # Script to merge datasets
│   ├── motor_dag.py              # DAG script for motor skills data processing
│   ├── motorsenses.ipynb         # Notebook for motor senses analysis
│   ├── participantstatus_demographics_biospecimen_dag.py # DAG script for participant status, demographics, and biospecimen
│   ├── pca.py                    # Script for principal component analysis
│   ├── preprocessing.py          # General preprocessing script
│   └── resampling.py             # Script for data resampling
├── dags/                         # DAGs for scheduling and automation
│   └── data_pipeline_new.py      # New data pipeline DAG script
├── logs/                         # Folder for logs
├── requirements.txt              # Dependencies and libraries
└── tests/                        # Test scripts
    ├── __init__.py               # Init file for test package
    ├── test_duplicate.py         # Unit test for handling duplicates
    ├── test_missing_value.py     # Unit test for handling missing values
    ├── test_outlier.py           # Unit test for outlier cleaning
    └── test_resampling.py        # Unit test for data resampling

```

## Description of the Data Pipeline Components:
• send_custom_alert_email: Sends a custom alert email if a task fails or is retried, with details about the task and DAG.  
• participant_status_load: Loads the "Participant_Status" CSV file into a DataFrame.  
• demographics_load: Loads the "Demographics" CSV file into a DataFrame.  
• clean_participant_status: Cleans the "Participant_Status" DataFrame by converting enrollment dates, renaming a column, and dropping unnecessary columns.  
• clean_demographics: Cleans the "Demographics" DataFrame by dropping columns that are not needed.  
• merge_participant_status_and_demographics: Merges the cleaned "Participant_Status" and "Demographics" DataFrames on the participant ID and filters rows with valid enrollment statuses.  
• clean_participantstatus_demographic: Further cleans the merged "Participant_Status" and "Demographics" DataFrame by dropping additional unnecessary columns.  
• biospecimen_analysis_load: Loads the "SAA_Biospecimen_Analysis_Results" CSV file into a DataFrame.  
• clean_biospecimen_analysis: Cleans the "Biospecimen_Analysis" DataFrame by formatting dates and dropping irrelevant columns.  
• filter_biospecimen_analysis: Filters the "Biospecimen_Analysis" DataFrame to keep only records with the earliest run date for baseline clinical events.  
• clean_filtered_biospecimen_analysis: Further cleans the filtered "Biospecimen_Analysis" DataFrame by dropping additional columns.  
• merge_participantstatus_demographics_biospecimen_analysis: Merges the cleaned "Participant_Status", "Demographics", and "Biospecimen_Analysis" DataFrames.  
• clean_participantstatus_demographics_biospecimen_analysis: Final cleanup of the merged DataFrame by dropping remaining unnecessary columns.  
• load_motor_senses_1: Loads the first motor senses CSV file into a DataFrame.  
• load_motor_senses_2: Loads the second motor senses CSV file into a DataFrame.  
• load_motor_senses_3: Loads the third motor senses CSV file into a DataFrame.  
• load_motor_senses_4: Loads the fourth motor senses CSV file into a DataFrame.  
• load_motor_senses_5: Loads the fifth motor senses CSV file into a DataFrame.  
• clean_motor_senses_1: Cleans the first motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_2: Cleans the second motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_3: Cleans the third motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_4: Cleans the fourth motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• clean_motor_senses_5: Cleans the fifth motor senses DataFrame by dropping unnecessary columns after retrieving it from XCom.  
• merge_all_motor_senses_csvs: Merges all cleaned motor senses DataFrames into a single DataFrame and pushes the merged DataFrame to XCom.  
• drop_duplicate_motor_senses_columns: Removes duplicate columns from the merged DataFrame and saves the final deduplicated DataFrame to a CSV file.  


## Airflow DAG components
![WhatsApp Image 2024-11-05 at 23 45 16_95809d38](https://github.com/user-attachments/assets/594b4ec5-9ee6-417f-8f67-6e16da2f5f2f)
![WhatsApp Image 2024-11-05 at 23 43 29_8baaf474](https://github.com/user-attachments/assets/cccacb79-1bf9-4546-8728-4b093913605e)

## Bias Detection and Mitigation
![1](https://github.com/user-attachments/assets/297ae311-24c7-4ad8-b4fd-77f18ef675d8)
![2](https://github.com/user-attachments/assets/1d49c8f4-faed-42d4-847a-41b6bdf44dc1)
![3](https://github.com/user-attachments/assets/e94069ca-dffc-4ef8-a931-b450374fa4e4)

Based on the generated graphs, the dataset shows several notable imbalances across age, gender, and cohort attributes. 

In the age distribution histogram, the majority of participants fall between ages 60 and 75, with relatively few data points under 50 or over 80. This concentration suggests that younger and older age groups are underrepresented, which could lead the model to perform well only within the dominant age range (60-75) and struggle with age extremes, potentially limiting accuracy for outliers in age-sensitive applications.

The gender distribution bar chart shows a clear imbalance, with approximately 70% of participants identified as one gender, while only 30% represent the other. This disparity could cause the model to favor predictions for the overrepresented gender, leading to biased performance that may be less accurate for the minority gender.

In the cohort distribution chart, one cohort comprises nearly 60% of the dataset, while other cohorts collectively represent the remaining 40%. Such an imbalance implies that the model may inadvertently learn patterns unique to the dominant cohort, resulting in reduced performance when predicting outcomes for less represented groups. Addressing these imbalances by enhancing the representation of underrepresented age groups, genders, and cohorts would help ensure that the model can learn fairly and perform consistently across all subgroups.

For mitigating these biases we can apply the following techniques:

1. Resampling: Apply oversampling to underrepresented age groups, gender, and cohorts to balance the dataset, or consider undersampling majority groups if data collection is limited.

2. Sample Weights: Assign higher weights to samples from underrepresented age groups, genders, and cohorts in the model’s training process, ensuring that each group contributes equally to model learning.

3. Stratified Splits: Use stratified sampling on ENROLL_AGE, SEX, and COHORT when dividing the dataset into training, validation, and test sets, maintaining balanced subgroup representation across all sets.

4. Feature Engineering: Add new categorical features, such as age bins (e.g., <50, 50-60, etc.) or cohort identifiers, to guide the model in recognizing specific patterns tied to age and cohort.

5. Bias and Fairness Metrics: During evaluation, check model performance metrics like accuracy and recall separately for each age group, gender, and cohort, and apply fairness metrics to detect and correct biases across these subgroups.
