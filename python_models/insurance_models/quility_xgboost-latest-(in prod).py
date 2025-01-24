import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import recall_score, accuracy_score, roc_auc_score, precision_score, f1_score, classification_report
from sklearn.impute import SimpleImputer
from xgboost import XGBClassifier
import joblib
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline

# 1. Use all three training files
file_path_1 = 'quility_train_updated.csv'
file_path_2 = 'qlm10162024_julymaileddata_without_full_name_20241022t075031_rwr_appended.csv'
file_path_3 = 'QLM10312024-AugustLeadsMarked-updated-with-BETTY-data.csv'
file_path_4 = 'octnovquilitytrainingdata_20250103t105626_rwr_appended.csv'

# Load all datasets
df1 = pd.read_csv(file_path_1, low_memory=False)
df2 = pd.read_csv(file_path_2, low_memory=False)
df3 = pd.read_csv(file_path_3, low_memory=False)
df4 = pd.read_csv(file_path_4, low_memory=False)

# Combine all datasets
df = pd.concat([df1, df2, df3], ignore_index=True) #

# uszips = pd.read_csv('uszips.csv', low_memory=False)
# uszips = uszips.rename(columns={'zip': 'mail_zip_code'})

def engineer_features(data):
    # 4. Use 'Presort Date' instead of current datetime
    # presort_date = pd.to_datetime(data['Presort Date'], errors='coerce')

    # date_columns = [
    #     # 'Mortgage Date',  # 2. Use 'Mortgage Date' instead of 'API_CurrentDeed_MortgageDate'
    #     # 'API_CurrentDeed_MortgageDueDate',
    #     # 'API_SaleInfo_AssessorLastSaleDate',
    #     # 'API_SaleInfo_LastOwnershipTransferDate'
    # ]

    # for col in date_columns:
    #     if col in data.columns:
    #         data[col] = pd.to_datetime(data[col], errors='coerce')

    new_features = {}

    # Existing features adjusted to use 'Presort Date'
    # new_features['Months_To_Mortgage_Due_Date'] = (data['API_CurrentDeed_MortgageDueDate'] - presort_date).dt.days / 30.44
    # new_features['Months_Since_Mortgage_Creation'] = (presort_date - data['Mortgage Date']).dt.days / 30.44  # 6. Updated feature
    # new_features['Months_Owned'] = (presort_date - data[['API_SaleInfo_AssessorLastSaleDate', 'API_SaleInfo_LastOwnershipTransferDate']].min(axis=1)).dt.days / 30.44
    # new_features['Mortgage_Period_Months'] = (data['API_CurrentDeed_MortgageDueDate'] - data['Mortgage Date']).dt.days / 30.44
    # new_features['Presence_Secondary_Owner'] = data[['API_PrimaryOwner_Name2First', 'API_PrimaryOwner_Name2Last']].notnull().any(axis=1).astype(int)

    # new_features['LTV_Ratio'] = data['Mortgage Amount'] / data['API_SaleInfo_DeedLastSalePrice'] * 100
    # new_features['Property_Age'] = presort_date.dt.year - data['API_PropertyUseInfo_YearBuilt']
    # new_features['Has_Second_Mortgage'] = (data['API_CurrentDeed_SecondMortgageAmount'] > 0).astype(int)

    # new_features['Missing_Mortgage_Due_Date'] = data['API_CurrentDeed_MortgageDueDate'].isnull().astype(int)
    # new_features['Missing_Mortgage_Date'] = data['Mortgage Date'].isnull().astype(int)  # Updated column name
    # new_features['Missing_LastSaleDate'] = data[['API_SaleInfo_AssessorLastSaleDate', 'API_SaleInfo_LastOwnershipTransferDate']].isnull().all(axis=1).astype(int)

    # # New potential_age related features
    # new_features['Age_30_50'] = ((data['potential_age'] >= 30) & (data['potential_age'] <= 50)).astype(int)
    # new_features['Age_Squared'] = data['potential_age'] ** 2
    new_features['Age_Log'] = np.log1p(data['potential_age'])

    # Age group buckets
    # new_features['Age_Group'] = pd.cut(data['potential_age'],
    #                                    bins=[0, 30, 40, 50, 60, 100],
    #                                    labels=['0-30', '31-40', '41-50', '51-60', '60+'])

    # # New feature: API_SaleInfo_DeedLastSalePrice / income_household_median
    # data = data.merge(uszips[['mail_zip_code', 'income_household_median']], on='mail_zip_code', how='left')
    # new_features['Price_to_Income_Ratio'] = data['API_SaleInfo_DeedLastSalePrice'] / data['income_household_median']

    # # 6. Create a new feature: difference between Mortgage Date and Presort Date
    # new_features['Mortgage_Presort_Diff_Days'] = (presort_date - data['Mortgage Date']).dt.days

    engineered_features = pd.DataFrame(new_features)
    return pd.concat([data, engineered_features], axis=1)

# Apply feature engineering
df = engineer_features(df)

# 5. Remove samples from training where Mortgage Amount equals 0
df = df[df['Mortgage Amount'] != 0]

# Update engineered_features list with updated column names
engineered_features = ['Age_Log','API_SaleInfo_DeedLastSalePrice','Mortgage Amount']

# Prepare the data for modeling
X = df[engineered_features]
y = df['Lead']

# Clean the data
def clean_data(X):
    X = X.replace([np.inf, -np.inf], np.nan)
    X = X.loc[:, X.isnull().mean() < 0.5]
    return X

X = clean_data(X)

# Get categorical columns and numeric columns
categorical_features = X.select_dtypes(include=['object', 'category']).columns.tolist()
numerical_features = X.select_dtypes(include=['int64', 'float64']).columns.tolist()

# 7. Update the preprocessor to handle cases where there are no categorical or no numeric features
preprocessor_steps = []

if numerical_features:
    numerical_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    preprocessor_steps.append(('num', numerical_pipeline, numerical_features))

if categorical_features:
    categorical_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])
    preprocessor_steps.append(('cat', categorical_pipeline, categorical_features))

# If there are no features, set preprocessor to 'passthrough'
if preprocessor_steps:
    preprocessor = ColumnTransformer(
        transformers=preprocessor_steps
    )
else:
    preprocessor = 'passthrough'

# Create a new pipeline including preprocessing and the classifier
pipeline = ImbPipeline([
    ('preprocessor', preprocessor),
    ('smote', SMOTE(random_state=42)),
    ('xgbclassifier', XGBClassifier(
        eval_metric='logloss',
        random_state=42
    ))
])

# Split the data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Fit the pipeline
pipeline.fit(X_train, y_train)

# Predict probabilities
y_pred_proba = pipeline.predict_proba(X_test)[:, 1]

# Make predictions with the custom threshold of 0.24
y_pred_default = (y_pred_proba > 0.24).astype(int)

# Evaluate the model with standard metrics
accuracy = accuracy_score(y_test, y_pred_default)
precision = precision_score(y_test, y_pred_default)
recall = recall_score(y_test, y_pred_default)
f1 = f1_score(y_test, y_pred_default)
auc_roc = roc_auc_score(y_test, y_pred_proba)

# Print the classification report for additional detail
print("\nClassification Report:")
print(classification_report(y_test, y_pred_default))

# Output the metrics
print(f"Accuracy: {accuracy:.2f}")
print(f"Precision: {precision:.2f}")
print(f"Recall: {recall:.2f}")
print(f"F1 Score: {f1:.2f}")
print(f"AUC-ROC: {auc_roc:.2f}")

# Save the XGBoost model in JSON format
xgb_model = pipeline.named_steps['xgbclassifier']
xgb_model.save_model('insurance_xgboost_model.json')

# Save the preprocessor if it's not 'passthrough'
if preprocessor != 'passthrough':
    joblib.dump(preprocessor, 'insurance_preprocessor.joblib')
else:
    # If preprocessor is 'passthrough', save a dummy object
    joblib.dump(None, 'insurance_preprocessor.joblib')

def load_xgboost_model_from_json(json_file):
    loaded_model = XGBClassifier()
    loaded_model.load_model(json_file)
    return loaded_model

# Assume we have already loaded the XGBoost model from JSON
loaded_xgb_model = load_xgboost_model_from_json('insurance_xgboost_model.json')

# Load the preprocessor separately
preprocessor_loaded = joblib.load('insurance_preprocessor.joblib')

# Function to preprocess new data
def preprocess_data(X, preprocessor):
    if preprocessor == 'passthrough' or preprocessor is None:
        return X
    else:
        X_preprocessed = preprocessor.transform(X)
        # Get feature names if possible
        try:
            feature_names = []
            if hasattr(preprocessor, 'transformers_'):
                for name, transformer, cols in preprocessor.transformers_:
                    if name == 'num':
                        feature_names.extend(transformer.named_steps['scaler'].get_feature_names_out(cols))
                    elif name == 'cat':
                        feature_names.extend(transformer.named_steps['onehot'].get_feature_names_out(cols))
            X_preprocessed_df = pd.DataFrame(X_preprocessed, columns=feature_names, index=X.index)
        except:
            # If feature names can't be retrieved, return as numpy array
            X_preprocessed_df = pd.DataFrame(X_preprocessed, index=X.index)
        return X_preprocessed_df

# Load the new test data
test_file_path = "octnovquilitytrainingdata_20250103t105626_rwr_appended.csv"
test_df = pd.read_csv(test_file_path, low_memory=False)

# Feature engineering for the new test data
test_df = engineer_features(test_df)

# Prepare the new test data for prediction
X_new_test = test_df[engineered_features]
X_new_test = clean_data(X_new_test)

X_new_test_preprocessed = preprocess_data(X_new_test, preprocessor_loaded)
y_pred_proba_new_test = loaded_xgb_model.predict_proba(X_new_test_preprocessed)[:, 1]

# Make predictions using the optimal threshold
y_pred_default_new_test = (y_pred_proba_new_test > 0.24).astype(int)

# Add BETTY SCORE and BETTY PREDICTED columns to the new test data
test_df['BETTY SCORE'] = (y_pred_proba_new_test * 100).round(2)
test_df['BETTY PREDICTED'] = y_pred_default_new_test

# Step 4: Use Ground Truth Data (if available)
# Ensure 'ground_truth' exists in test data
if 'ground_truth' in test_df.columns:
    test_df['Lead_TRUE'] = test_df['ground_truth'].astype(int)

    # Calculate metrics
    positive_predictions_for_true_leads = test_df[(test_df['BETTY PREDICTED'] == 1) & (test_df['Lead_TRUE'] == 1)].shape[0]
    total_true_leads = test_df['Lead_TRUE'].sum()
    percentage_correctly_predicted = 100 * positive_predictions_for_true_leads / total_true_leads if total_true_leads > 0 else 0

    total_leads = test_df['BETTY PREDICTED'].sum()
    percentage_eliminated = 100 * (1 - total_leads / len(test_df)) if len(test_df) > 0 else 0

    # Bottom 10%, 20%, 30%, 40% Calculations
    sorted_df = test_df.sort_values(by='BETTY SCORE', ascending=True)

    for percent in [10, 20, 30, 40]:
        bottom_df = sorted_df.head(int((percent/100) * len(sorted_df)))
        missed_true_count = bottom_df['Lead_TRUE'].sum()
        percentage_trues_missed = 100 * missed_true_count / total_true_leads if total_true_leads > 0 else 0
        last_betty_score = bottom_df['BETTY SCORE'].iloc[-1] if not bottom_df.empty else np.nan
        print(f"Percentage TRUEs missed in bottom {percent}%: {percentage_trues_missed:.2f}%")
        print(f"Threshold for bottom {percent}% : {last_betty_score:.2f}")

    print(f"Percentage eliminated in file: {percentage_eliminated:.2f}%")
    print(f"Percentage correctly predicted: {percentage_correctly_predicted:.2f}%")
else:
    print("Ground truth data ('ground_truth' column) not available in the test data.")

# Save the updated test file with predictions
# test_df.to_csv('aug-file-AFTER-using-aug-data-for-training.csv', index=False)
