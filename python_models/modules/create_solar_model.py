import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from modules.solar_utils import SolarDataProcessor
import numpy as np

class SolarModel:
    def __init__(self, with_solar_folder, without_solar_folder):
        self.data_processor = SolarDataProcessor(with_solar_folder, without_solar_folder)
        self.model = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss')

    def prepare_data(self):
        self.data_processor.load_data()
        # self.data_processor.clean_data()
        common_columns = self.data_processor.find_common_columns()
        self.data_processor.concatenate_dataframes(common_columns)

        columns_to_drop = [
            'mail_street_address', 'BETTY_ROOF_SCORE', 'latitude', 'BETTY_UPDATED_ADDRESS_ZIP', 'geocode_longitude',
            'fullAddress', 'BETTY SCORE', 'full_name_missing', 'age_source', 'mail_city', 'geocode_latitude',
            'BETTY_UPDATED_ADDRESS_CITY', 'formatted_address', 'property_zip_code', 'mail_zip_code', 'longitude',
            'demo_address_verification_failed', 'mail_state_name_short_code', 'owner_full_name', 'BETTY_UPDATED_ADDRESS_STATE',
            'BETTY_DEMOGRAPHIC_SCORE', 'gender_source', 'BETTY_UPDATED_ADDRESS_LINE2', 'address_validation_source',
            'name', 'BETTY_UPDATED_ADDRESS_LINE1'
        ]

        self.data_processor.drop_columns(columns_to_drop)
        self.data_processor.save_dataframes(r'S:\reworked\with_solar_cleaned.csv', r'S:\reworked\without_solar_cleaned.csv')

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

        self.data_processor.convert_columns(None, mappings)
        self.data_processor.save_converted_dataframes(r'S:\reworked\with_solar_converted.csv', r'S:\reworked\without_solar_converted.csv')
        self.data_processor.combine_dataframes()
        self.data_processor.drop_non_numeric_columns()  # Drop non-numeric columns before calculating correlation matrix
        correlation_matrix = self.data_processor.calculate_correlation_matrix()
        self.data_processor.save_correlation_matrix(correlation_matrix, r'S:\reworked\correlation_matrix.csv')
        self.data_processor.save_final_dataframes(r'S:\reworked\with_solar_cleaned_final.csv', r'S:\reworked\without_solar_cleaned_final.csv')

    def train_model(self):
        combined_df_with_solar = pd.read_csv(r'S:\reworked\with_solar_cleaned_final.csv')
        combined_df_without_solar = pd.read_csv(r'S:\reworked\without_solar_cleaned_final.csv')
        combined_df = pd.concat([combined_df_with_solar, combined_df_without_solar], ignore_index=True)
        X = combined_df[self.get_selected_features()]
        y = combined_df['solar_installed']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        self.model.fit(X_train, y_train)

        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        print(f"Accuracy: {accuracy:.4f}")
        print(classification_report(y_test, y_pred))
        print(confusion_matrix(y_test, y_pred))

        self.model.save_model(r'S:\reworked\solar_XGB_model.json')
        print("Model saved to S:>reworked>solar_XGB_model.json")

    @staticmethod
    def get_selected_features():
        correlation_matrix = pd.read_csv(r'S:\reworked\correlation_matrix.csv', index_col=0)
        correlations = correlation_matrix['solar_installed'].abs()
        selected_features = correlations[correlations > 0.1].index.tolist()
        
        if 'solar_installed' in selected_features:
            selected_features.remove('solar_installed')
        
        return selected_features


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train Solar Panel Adoption Prediction Model")
    # PATHS NEED TO BE CHANGED FOR RECREATING THE MODEL
    parser.add_argument('--with_solar_folder', type=str, default=r'S:\reworked\With_Solar', help='Path to the folder containing data with solar')
    parser.add_argument('--without_solar_folder', type=str, default=r'S:\reworked\Without_Solar', help='Path to the folder containing data without solar')

    args = parser.parse_args()

    model = SolarModel(args.with_solar_folder, args.without_solar_folder)
    model.prepare_data()
    model.train_model()
