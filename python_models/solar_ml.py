import pandas as pd
import sys
import logging
import json
from ml_scoring_for_solar import SolarPredictor
import modules.rules_engine_solar as rule_engine_solar
import os

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
    
    # Condition 5: 'demo_address_verification_failed' is TRUE for more than 50%
    return confidence_dict

logging.basicConfig(level=logging.INFO, format="%(message)s")

try:
    # Script Starts Here    
    industry_profile = {}

    model_dir = os.path.dirname(os.path.abspath(__file__))
    model_filename = os.path.join("solar_models", "solar_XGB_model.json")
    model_path = os.path.join(model_dir, model_filename)

    for i, arg in enumerate(sys.argv):
        if arg == '--file_path':
            file_path = sys.argv[i + 1]
        if arg == "--industry_profile":
            industry_profile = json.loads(sys.argv[i + 1])

    new_file_path = file_path.split('.csv')[0] + '_betty.csv'

    # Initialize SolarPredictor
    predictor = SolarPredictor(model_path)

    # Load data
    df = pd.read_csv(file_path, low_memory=False, on_bad_lines='warn')

    # Apply rule engine to the DataFrame
    df = rule_engine_solar.rule_engine_solar(df)

    # Process and get BETTY SCORE
    df = predictor.ml_scoring_for_solar(df)

    # Calculate confidence score
    confidence_dict = calculate_confidence_score(df)

    # Save the final DataFrame
    df.to_csv(new_file_path, index=False)
    logging.info({"success": "True", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "ConfidenceDict": confidence_dict})

except Exception as e:
    exc = str(e).replace('"', '').replace("'", '')
    logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": exc})
