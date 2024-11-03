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


