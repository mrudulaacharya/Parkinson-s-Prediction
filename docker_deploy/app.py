from flask import Flask, request, jsonify, render_template
import joblib
import numpy as np
import pandas as pd
import os

# Load preprocessing objects
with open('preprocessor.pkl', 'rb') as file:
    preprocessor = joblib.load(file)

# Load model
with open('model.pkl', 'rb') as file:
    model = joblib.load(file)

app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

@app.route('/')
def home():
    return render_template('index.html')  # Render the HTML form

@app.route('/health')
def health():
    return jsonify({"status": "healthy"}), 200
    
@app.route('/predict', methods=['POST'])
def predict():
    input_data = request.form.get('input_data')
        
    input_list = [float(x.strip()) for x in input_data.split(',')]
   
    expected_columns = preprocessor.feature_names_in_
    df = pd.DataFrame([input_list], columns= expected_columns)    
    
    input=preprocessor.transform(df)
    categorical_cols = preprocessor.transformers_[1][2]  # Categorical column names
    one_hot_feature_names = preprocessor.transformers_[1][1].named_steps['one_hot'].get_feature_names_out(categorical_cols)
    numerical_cols = preprocessor.transformers_[0][2]  # Numerical column names
    remaining_cols = [col for col in df.columns if col not in numerical_cols + list(categorical_cols)]

        # Combine column names for the resulting DataFrame
    all_feature_names = list(numerical_cols) + one_hot_feature_names.tolist() + remaining_cols

    preprocessed_df = pd.DataFrame(input.toarray(), columns=all_feature_names)

    # Make prediction
    prediction = model.predict(preprocessed_df)

    maps={0:"Parkinson's Disease",1:"Healthy Control",2:"SWEDD",3:"Prodromal"}
    return render_template('index.html', prediction=maps[prediction[0]])  # Show prediction on the page
    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
