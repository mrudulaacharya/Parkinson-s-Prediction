# Parkinson-s-Prediction

This project focuses on building a end-to-end machine learning pipeline to detect Parkinson's Disease and its prognosis using motor symptoms, biospecimen analysis and patient status and demographic data. This README provides comprehensive setup instructions, a guide to running the pipeline, and an explanation of the project structure.

## Table of Contents
- [Project Overview](#project-overview)
- [Environment Setup](#environment-setup)
- [Running the Pipeline](#running-the-pipeline)
- [Code Structure](#code-structure)


## Project Overview
The aim of this project is to detect Parkinson's Disease using machine learning techniques. The pipeline includes data preprocessing, feature extraction, model training, evaluation, and bias detection and mitigation.

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

## Code Structure 
## Project Folder Structure

```plaintext
Parkinsons-Prediction/
├── README.md                     # Project documentation
├── data/                         # Folder for datasets
   ├── cleaned_data.csv           # Raw, unprocessed data 
├── scripts/                      # Scripts for data handling and preprocessing
   ├── DataProfiling.ipynb        # Data profiling notebook
   ├── clean_outlier.py           # Script to clean outliers
   ├── correlation.py             # Script for correlation analysis
   ├── data_merging.ipynb         # Notebook for data merging
   ├── data_schema.ipynb          # Notebook for defining data schema
   ├── drop_columns.py            # Script to drop unnecessary columns
   ├── duplicates_handler.py      # Script to handle duplicate data
   ├── merge_data.py              # Script to merge datasets
   ├── missing_values_handler.py  # Script to handle missing values
   ├── preprocessing.py           # General preprocessing script
   └── resampling.py              # Script for data resampling
├── requirements.txt              # Dependencies and libraries
└── tests/                        # Test scripts
   ├── __init__.py                # Init file for test package
   ├── test_duplicate.py          # Unit test for handling duplicates
   ├── test_missing_value.py      # Unit test for handling missing values
   ├── test_outlier.py            # Unit test for outlier cleaning
   └── test_resampling.py         # Unit test for data resampling

```

## Code Explanations

- merge_data.py : Iterates through all CSV files in a specified directory, loads each file into a pandas DataFrame, and stores them in a list. It then merges these DataFrames on the common column PATNO using an inner join, resulting in a single merged DataFrame.

- drop_columns.py : Drops columns that seem irrelevant to the prediction of target : 'COHORT'

- preprocessing.py: This script performs data cleaning, preprocessing, and exploratory data analysis (EDA) on a dataset. It cleans the data by handling duplicates and missing values, preprocesses it by converting date columns and encoding categorical variables, and visualizes distributions for specific columns like ENROLL_AGE, SEX, and COHORT.

- correlation.py : Loads a CSV file into a DataFrame, selects only the numeric columns, and calculates the correlation matrix for these columns. It then visualizes the correlation matrix as a heatmap without annotation, showing the strength of relationships between numerical features in the dataset.

- resampling.py : Categorizes participants into specified age groups, and balances the age distribution by under-sampling the '60-69' group and over-sampling the '80 and above' group to match the count of the '70-79' group.


## Bias Detection and Mitigation

Detecting Bias - The dataset contains 136 columns, with a mix of numerical and text-based columns. To detect bias, we can start by examining the following columns that may represent demographic or categorical attributes:

1) COHORT - Indicates participant groups, which might represent a categorical variable.
2) ENROLL_AGE - Enrollment age, a continuous variable, which can be grouped into age brackets for analysis.

Mitigation of Bias - To address potential biases arising from imbalanced age groups in the dataset, re-sampling strategy to balance representation across key age groups.
The age brackets are: Under 50, 50-59, 60-69, 70-79, and 80 and above. We set the 70-79 age group as the target size, serving as a baseline for balancing. The 60-69 group, which exceeded this size, was downsampled to reduce its representation, while the 80 and above group, which had fewer participants, was oversampled to increase its representation. This approach created a more uniform age distribution, improving fairness in the dataset.

