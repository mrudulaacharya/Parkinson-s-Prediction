# Parkinson-s-Prediction

This project focuses on building a machine learning pipeline to detect Parkinson's Disease using audio features, motor and non-motor symptoms, or other biomarker data. This README provides comprehensive setup instructions, a guide to running the pipeline, and an explanation of the project structure.

## Table of Contents
- [Project Overview](#project-overview)
- [Environment Setup](#environment-setup)
- [Running the Pipeline](#running-the-pipeline)
- [Code Structure](#code-structure)
- [Dataset](#dataset)
- [Acknowledgments](#acknowledgments)

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
   git clone https://github.com/your-username/parkinsons-detection.git
   cd parkinsons-detection

## Dataset

## Code Structure 
parkinsons-detection/  
│  
├── dags/                           
│   ├── raw_data.csv                
│   ├── processed_data.csv           
│   └── test_data.csv                
│  
├── data/                           
│   └── parkinsons_model.h5           
│  
├── scripts/                           
│   └── bias_report.json             
│  
├── tests/                       
│   └── exploratory_analysis.ipynb     
│  
├── logs/                           
│   ├── preprocess_data.py          
│   ├── train_model.py              
│   ├── evaluate_model.py            
│   └── bias_detection.py         
│  
├── README.md                       # Project README file  
├── requirements.txt                # Project dependencies  
└── .gitignore                      # Git ignore file  


## Code Explanations

- merge_data.py : Iterates through all CSV files in a specified directory, loads each file into a pandas DataFrame, and stores them in a list. It then merges these DataFrames on the common column PATNO using an inner join, resulting in a single merged DataFrame.

- drop_columns.py : Drops columns that seem irrelevant to the prediction of target : 'COHORT'

- correlation.py : Loads a CSV file into a DataFrame, selects only the numeric columns, and calculates the correlation matrix for these columns. It then visualizes the correlation matrix as a heatmap without annotation, showing the strength of relationships between numerical features in the dataset.

- resampling.py : Categorizes participants into specified age groups, and balances the age distribution by under-sampling the '60-69' group and over-sampling the '80 and above' group to match the count of the '70-79' group.


##  Bias Detection and Mitigation of Bias

This ananlysis detects potential biases across different age groups by segmenting the data into age bins (Under 50, 50-59, 60-69, 70-79, 80 and above). By analyzing the model's performance metrics such as accuracy, precision, and recall within each age group, we identified discrepancies that suggested age-related biases. Iterative monitoring of these age slices helped ensure consistent performance, with significant deviations highlighting potential areas of concern. Future work includes using fairness tools to automate bias detection, ensuring model fairness as the dataset evolves.

