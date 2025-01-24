import pandas as pd
import numpy as np
import joblib
from datetime import datetime
from xgboost import XGBClassifier
import os

class InsurancePredictor:
    def __init__(self, model_path, preprocessor_path):
        """
        Initialize the Insurance Predictor with pre-trained model and preprocessor
        
        Args:
            model_path: Path to the saved XGBoost model file
            preprocessor_path: Path to the saved feature preprocessor
        """
        # Load the pre-trained XGBoost model for prediction
        self.model = XGBClassifier()
        self.model.load_model(model_path)
        
        # Load the feature preprocessor that standardizes/normalizes input data
        self.preprocessor = joblib.load(preprocessor_path)
        
        # List of features required for prediction, including engineered ones
        self.engineered_features = ['Age_Log', 'API_SaleInfo_DeedLastSalePrice', 'Mortgage Amount']
        
    def load_data(self, file_path):
        """
        Load data from a CSV file.
        """
        data = pd.read_csv(file_path, low_memory=False)
        return data

    def engineer_features(self, data):
        """
        Create additional features needed for accurate predictions
        
        Args:
            data: DataFrame containing raw input features
            
        Returns:
            DataFrame with both original and newly engineered features
        """
        # Dictionary to store newly created features
        new_features = {}
        
        # Apply log transformation to potential_age
        new_features['Age_Log'] = np.log1p(data['potential_age'])
        
        # Combine original data with engineered features
        engineered_features = pd.DataFrame(new_features)
        return pd.concat([data, engineered_features], axis=1)

    def clean_data(self, X):
        """
        Clean and filter features to ensure data quality
        
        Args:
            X: DataFrame of features to be cleaned
            
        Returns:
            DataFrame with cleaned features, removing infinite values and high-missing columns
        """
        # Replace infinite values with NaN to handle mathematical errors
        X = X.replace([np.inf, -np.inf], np.nan)
        
        # Remove columns where more than 50% of values are missing
        X = X.loc[:, X.isnull().mean() < 0.5]
        return X

    def preprocess_data(self, X):
        """
        Preprocess data using production logic
        """
        if self.preprocessor == 'passthrough' or self.preprocessor is None:
            return X
      
        # Transform the data
        transformed_features = self.preprocessor.transform(X)
        
        # Convert to DataFrame with same index as input
        return pd.DataFrame(transformed_features, index=X.index)

    def predict_leads(self, data):
        """
        Generate insurance lead scores using the trained model
        
        Args:
            data: DataFrame containing customer information
            
        Returns:
            DataFrame with original data plus BETTY SCORE predictions
            
        Raises:
            Exception: If any step in the prediction pipeline fails
        """
        try:
            # Step 1: Create necessary engineered features
            input_data = self.engineer_features(data)
            
            # Step 2: Extract only the features needed for prediction
            feature_subset = input_data[self.engineered_features]
            feature_subset = self.clean_data(feature_subset)
            
            # Step 3: Apply feature preprocessing (scaling/normalization)
            processed_features = self.preprocess_data(feature_subset)
            
            # Step 4: Generate probability predictions using the model
            prediction_probabilities = self.model.predict_proba(processed_features)[:, 1]
            
            # Step 5: Convert probabilities to scores (0-100 scale) and add to results
            input_data['BETTY SCORE'] = (prediction_probabilities * 100).round(2)
            return input_data
            
        except Exception as e:
            print(f"Error in predict_leads: {e}")
            raise
