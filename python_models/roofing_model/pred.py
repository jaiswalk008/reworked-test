import pandas as pd
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import joblib
import warnings
warnings.filterwarnings('ignore')

# Load the saved model and artifacts
print("Loading model artifacts...")
model_artifacts = joblib.load('roof_work_prediction_model_artifacts.joblib')
model = model_artifacts['model']
feature_names = model_artifacts['feature_names']

# Load both datasets
print("Loading data...")
negative_cases = pd.read_csv('2k_roof_20241223t085234_rwr_appended_betty.csv', low_memory=False)
positive_cases = pd.read_csv('cleaned_train_with_solar_and_property_data.csv', low_memory=False)

def prepare_features_for_prediction(df, feature_names):
    X = pd.DataFrame(index=df.index)
    for col in feature_names:
        if col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                X[col] = df[col].fillna(df[col].median())
            else:
                X[col] = df[col].fillna('Unknown').astype(str)
                le = LabelEncoder()
                X[col] = le.fit_transform(X[col])
        else:
            X[col] = 0
    X = X[feature_names]
    return X

def analyze_predictions(data, dataset_name):
    print(f"\n=== Analysis for {dataset_name} ===")
    
    # Prepare features and make predictions
    X = prepare_features_for_prediction(data, feature_names)
    predictions = model.predict(X)
    
    # Compare with actual values
    if 'roof_work_done' in data.columns:
        actual = data['roof_work_done'].astype(bool)
        correct_predictions = sum(predictions == actual)
        
        print(f"Total records: {len(data)}")
        print(f"Correct predictions: {correct_predictions}")
        print(f"Accuracy: {(correct_predictions/len(data))*100:.2f}%")
        
        # Print confusion matrix
        cm = confusion_matrix(actual, predictions)
        print("\nConfusion Matrix:")
        print("            Predicted NO  Predicted YES")
        print(f"Actually NO     {cm[0][0]}          {cm[0][1]}")
        print(f"Actually YES    {cm[1][0]}          {cm[1][1]}")

# Run analysis on both datasets
print("\nAnalyzing negative cases (should all be FALSE)...")
analyze_predictions(negative_cases, "negative_cases")

print("\nAnalyzing positive cases (should all be TRUE)...")
analyze_predictions(positive_cases, "positive_cases")