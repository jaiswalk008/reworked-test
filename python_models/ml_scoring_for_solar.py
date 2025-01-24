import pandas as pd
import numpy as np
import xgboost as xgb
# from modules.create_solar_model import SolarModel
from modules.solar_utils import get_selected_features

class SolarPredictor():
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = xgb.XGBClassifier()
        self.model.load_model(model_path)

    def predict(self, data):
        processed_data = self.process_input_data(data)
        score = self.model.predict_proba(processed_data)[:, 1] * 100
        return score

    def process_input_data(self, data):
        mappings = {
            'solarPotential.financialAnalyses.leasingSavings.leasesAllowed': {True: 1, np.nan: 0, 'TRUE': 1, 'True': 1, '0': 0},
            'solarPotential.financialAnalyses.leasingSavings.leasesSupported': {True: 1, np.nan: 0, 'TRUE': 1, 'True': 1, '0': 0},
            'ownrent': {'Definite Owner': 4, 'Probable Owner': 3, 'Probable Renter': 2, 'Definite Renter': 1, np.nan: 0},
            'maritalstatus': {'Definitely Married': 4, 'Possibly Married': 3, 'Definitely Single': 2, 'Possibly Single': 1, np.nan: 0},
            'householdincome': {'$0-$15,000': 1, '$15,001-$20,000': 2, '$20,001-$30,000': 3, '$30,001-$40,000': 4, '$40,001-$50,000': 5,
                                '$50,001-$60,000': 6, '$60,001-$75,000': 7, '$75,001-$100,000': 8, '$100,001-$125,000': 9,
                                '$125,001-$150,000': 10, '$150,001+': 11, np.nan: 0},
            'education': {'High School': 1, 'College': 2, 'Vocational/Technical School': 3, 'Graduate School': 4, np.nan: 0},
            'presenceofchildren': {'Children Present': 1, 'No Children Present': 0, np.nan: 0},
            'demo_gender': {'M': 1, 'F': 2, 'unknown': 0, np.nan: 0},
            'politicalparty': {'Republican': 4, 'Democrat': 3, 'Independent': 2, 'No Party': 1, np.nan: 0},
            'lengthofresidence': {'Less than 1 year': 0.5, '1-2 years': 1.5, '2-3 years': 2.5, '3-4 years': 3.5,
                                '4-5 years': 4.5, '5-6 years': 5.5, '6-7 years': 6.5, '7-8 years': 7.5, '8-9 years': 8.5,
                                '9-10 years': 9.5, '10-11 years': 10.5, '11-12 years': 11.5, '12-13 years': 12.5,
                                '13-14 years': 13.5, '14-15 years': 14.5, '15+ years': 16, np.nan: 0},
            'solarPotential.financialAnalyses.financialDetails.netMeteringAllowed': {True: 1, np.nan: 0, 'TRUE': 1, 'True': 1, '0': 0}
        }

        for column, mapping in mappings.items():
            if column in data.columns:
                data[column] = data[column].map(mapping).fillna(0).astype(int)

        selected_features = get_selected_features()
        return data[selected_features]

    def ml_scoring_for_solar(self, data):
        try:
            # Make predictions
            data['BETTY SCORE'] = self.predict(data).round(2) # rounded to 2 decimal places
            
            return data
        except Exception as e:
            exc = str(e).replace('"','').replace("'",'')
            raise RuntimeError(f"Processing failure: {exc}")
