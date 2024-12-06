from flask import Flask, request, jsonify
import pickle
import os
from google.cloud import storage
import numpy as np
import joblib

# Global variables to store the model and its type
model = None
model_type = None


def load_model():
    """Load the model from Google Cloud Storage."""
    global model, model_type
    if model is None:  # Check if the model is already loaded
        try:
            bucket_name = os.environ.get("BUCKET_NAME")
            if not bucket_name:
                raise ValueError("Environment variable 'BUCKET_NAME' is not set.")

            file_name = "model.pkl"  # Path to the model file in the bucket

            # Log the bucket and file name
            print(f"Bucket Name: {bucket_name}")
            print(f"Model File: {file_name}")

            # Initialize Google Cloud Storage client
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_name)

            # Load the model
            print(f"Loading model from gs://{bucket_name}/{file_name}")
            with blob.open("rb") as f:
                loaded_model = joblib.load(f)

            # Detect model type
            if hasattr(loaded_model, "predict") and hasattr(loaded_model, "fit"):
                if "xgboost" in str(type(loaded_model)).lower():
                    model_type = "xgboost"
                elif "svm" in str(type(loaded_model)).lower():
                    model_type = "svm"
                elif "randomforest" in str(type(loaded_model)).lower():
                    model_type = "rf"
                elif "logisticregression" in str(type(loaded_model)).lower():
                    model_type = "logreg"
                else:
                    model_type = "sklearn"
                model = loaded_model
                print(f"Loaded model type: {model_type}")
            else:
                raise ValueError(f"Unsupported model type: {type(loaded_model)}")

            print("Model loaded successfully.")
        except Exception as e:
            print(f"Error loading model: {e}")
            raise e


def predict(request):
    try:
        # Ensure the model is loaded
        load_model()

        # Parse JSON input
        data = request.get_json()
        if "data" not in data:
            return jsonify({"error": "Invalid input. Expected key 'data'."}), 400

        # Validate input shape
        input_data = data["data"]
        if not isinstance(input_data, list) or not all(isinstance(row, list) for row in input_data):
            return jsonify({"error": "Input data must be a 2D list."}), 400

        # Convert input to NumPy array
        input_array = np.array(input_data)

        # Perform prediction based on model type
        if model_type in ["xgboost", "rf", "logreg", "svm"]:
            predictions = model.predict(input_array)

            # If predict_proba is available, include probabilities
            response = {"predictions": predictions.tolist()}
            if hasattr(model, "predict_proba") and model_type != "svm":  # SVM predict_proba is not always available
                probabilities = model.predict_proba(input_array)
                response["probabilities"] = probabilities.tolist()

            return jsonify(response)
        else:
            return jsonify({"error": f"Unsupported model type: {model_type}"}), 500

    except Exception as e:
        print(f"Error during prediction: {e}")
        return jsonify({"error": str(e)}), 500


# Flask app initialization
app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict_route():
    return predict(request)

