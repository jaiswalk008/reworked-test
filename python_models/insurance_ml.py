import pandas as pd
import numpy as np
import joblib
from datetime import datetime
import sys
import logging
import json
import os
from InsurancePredictor import InsurancePredictor

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
    demo_address_verification_failed = (df['demo_address_verification_failed'] == True).mean() * 100
    confidence_dict['demo_address_verification_failed'] = round(demo_address_verification_failed)
    if demo_address_verification_failed > 50:
        reduction = 40
        confidence_score -= reduction

    confidence_dict['confidence_score'] = max(0, confidence_score)
    return confidence_dict

logging.basicConfig(level=logging.INFO, format="%(message)s")

try:
    # Script Starts Here    
    industry_profile = {}
    file_path = ''
    new_file_path = ''

    script_dir = os.path.dirname(os.path.abspath(__file__))
    logging.info(f"Script directory: {script_dir}")

    model_filename = os.path.join("insurance_models", "insurance_xgboost_model.json")
    model_path = os.path.join(script_dir, model_filename)
    preprocessor_path = os.path.join(script_dir, "insurance_models", "insurance_preprocessor.joblib")

    logging.info(f"Model path: {model_path}")
    logging.info(f"Preprocessor path: {preprocessor_path}")
    logging.info(f"Expected uszips.csv path: {os.path.join(script_dir, 'lead_sorting', 'uszips.csv')}")

    for i, arg in enumerate(sys.argv):
        if arg == '--file_path':
            file_path = sys.argv[i + 1]
        if arg == "--industry_profile":
            industry_profile = json.loads(sys.argv[i + 1])

    if not file_path:
        raise ValueError("File path not provided")

    logging.info(f"Input file path: {file_path}")

    new_file_path = file_path.split('.csv')[0] + '_betty.csv'
    logging.info(f"Output file path: {new_file_path}")

    # Check if the industry type is insurance
    if industry_profile.get('industryType') != 'insurance_provider':
        raise ValueError("Industry type is not insurance_provider")

    # Initialize InsurancePredictor
    try:
        predictor = InsurancePredictor(model_path, preprocessor_path)
    except FileNotFoundError as e:
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": f"File not found: {str(e)}"})
        # Check if required files exist and log their presence
        for file_path in [model_path, preprocessor_path, os.path.join(script_dir, 'lead_sorting', 'uszips.csv')]:
            if os.path.exists(file_path):
                logging.info(f"File exists: {file_path}")
            else:
                logging.error(f"File does not exist: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": f"Error initializing InsurancePredictor: {str(e)}"})
        sys.exit(1)

    # Load data
    try:
        df = pd.read_csv(file_path, low_memory=False, on_bad_lines='warn')
    except Exception as e:
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": f"Error loading data: {str(e)}"})
        sys.exit(1)

    # Process and get BETTY SCORE
    try:
        df = predictor.predict_leads(df)
    except Exception as e:
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": f"Error in predict_leads: {str(e)}"})
        sys.exit(1)

    # Calculate confidence score
    try:
        confidence_dict = calculate_confidence_score(df)
    except Exception as e:
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": f"Error calculating confidence score: {str(e)}"})
        sys.exit(1)

    # Save the final DataFrame
    try:
        df.to_csv(new_file_path, index=False)
        logging.info({"success": "True", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "ConfidenceDict": confidence_dict})
    except Exception as e:
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": f"Error saving results: {str(e)}"})
        sys.exit(1)

except Exception as e:
    exc = str(e).replace('"', '').replace("'", '')
    logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": new_file_path, "error": "betty_failure", "error_details": exc})
    sys.exit(1)