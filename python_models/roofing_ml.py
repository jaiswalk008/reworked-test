import pandas as pd
import sys
import logging
import json
from preprocess import is_apartment
from weather_api import WeatherEventsFetcher
import os
import numpy as np
import joblib
from sklearn.preprocessing import LabelEncoder

def calculate_confidence_score(df):
    confidence_dict = {}
    confidence_score = 100
    
    confidence_dict['confidence_score'] = confidence_score
    
    # Calculate percentages for each condition
    age_source_melissa = (df['age_source'] == 'Melissa').mean() * 100
    
    # Condition 1: 'age_source_melissa' is filled with mode(age_source_melissa) for more than 40%
    confidence_dict['age_source_melissa'] = round(age_source_melissa)
    if age_source_melissa < 40:
        reduction = 60
        confidence_score -= reduction
    
    return confidence_dict

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

def calculate_weather_score(df, api_key="E33PLV6G3B83PN9L77BVESPN8"):
    weather_fetcher = WeatherEventsFetcher(api_key)
    
    def check_weather_events(row):
        events = weather_fetcher.get_weather_events(
            row.get('latitude', 0), 
            row.get('longitude', 0)
        )
        score_increase = 0
        if events:
            # Calculate base score from XGBoost prediction
            base_score = row.get('BETTY SCORE', 0)
            
            # Calculate maximum possible increase while keeping total under 100
            max_increase = min(20, 100 - base_score)  # Cap total increase at 20 points
            
            # Additional points for specific events, weighted by severity
            event_types = sum([event['event_types'] for event in events], [])
            if 'hail' in event_types:
                score_increase += min(10, max_increase * 0.5)  # 50% of available increase
            if 'storm' in event_types:
                score_increase += min(7, max_increase * 0.35)  # 35% of available increase
            if 'high wind' in event_types:
                score_increase += min(5, max_increase * 0.25)  # 25% of available increase
                
            # Ensure total increase doesn't exceed max_increase
            score_increase = min(score_increase, max_increase)
        return score_increase

    df['weather_score'] = df.apply(check_weather_events, axis=1)
    df['BETTY SCORE'] = df['BETTY SCORE'] + df['weather_score']
    return df

logging.basicConfig(level=logging.INFO, format="%(message)s")

try:
    # Script Starts Here    
    industry_profile = {}

    for i, arg in enumerate(sys.argv):
        if arg == '--file_path':
            file_path = sys.argv[i + 1]
        if arg == "--industry_profile":
            industry_profile = json.loads(sys.argv[i + 1])

    new_file_path = file_path.split('.csv')[0] + '_betty.csv'

    # Load XGBoost model artifacts
    model_dir = os.path.dirname(os.path.abspath(__file__))
    model_artifacts = joblib.load(os.path.join(model_dir, 'roofing_model/roofing_xgb_model_artifacts.joblib'))
    model = model_artifacts['model']
    feature_names = model_artifacts['feature_names']

    # Load data
    df = pd.read_csv(file_path, low_memory=False, on_bad_lines='warn')

    # Step 1: Check for apartments and adjust score
    df['is_apartment'] = df.apply(lambda row: is_apartment(row.get('property_address', '') or row.get('BETTY_UPDATED_ADDRESS_LINE1', ''), row.get('API_PropertyUseInfo_PropertyUseGroup', '')), axis=1)
    
    # Step 2: Prepare features and get XGBoost predictions
    X = prepare_features_for_prediction(df, feature_names)
    predictions_proba = model.predict_proba(X)
    
    # Convert probabilities to Betty scores (0-100)
    df['BETTY SCORE'] = (predictions_proba[:, 1] * 100).astype(int)
    
    # Set score to 0 for apartments
    df.loc[df['is_apartment'], 'BETTY SCORE'] = 0

    # Step 3: Apply weather score adjustments
    # df = calculate_weather_score(df)
    
    # Ensure final scores don't exceed 100
    df['BETTY SCORE'] = df['BETTY SCORE'].clip(0, 100)

    # Calculate confidence score
    confidence_dict = calculate_confidence_score(df)

    # Save the final DataFrame
    df.to_csv(new_file_path, index=False)
    logging.info({"success": "True", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "ConfidenceDict": confidence_dict})

except Exception as e:
    exc = str(e).replace('"', '').replace("'", '')
    logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": exc})