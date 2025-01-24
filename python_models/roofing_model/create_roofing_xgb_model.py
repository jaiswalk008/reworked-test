import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings('ignore')

# Load the datasets
print("Loading datasets...")
positive_cases = pd.read_csv('cleaned_train_with_solar_and_property_data.csv', low_memory=False)
negative_cases = pd.read_csv('2k_roof_20241223t085234_rwr_appended_betty.csv', low_memory=False)

# Define features to keep
features_to_keep = [
    # Property Characteristics
    'year_built',
    'property_type',
    'propimprovedvalue',
    'proptotalvalue',
    'proplandvalue',
    'proplotsize',
    
    # Demographic Information
    'potential_age',
    'householdincome',
    'householdsize',
    'lengthofresidence',
    'education',
    'ownrent',
    'presenceofchildren',
    'maritalstatus',
    
    # Property Usage/Status
    'API_PropertyUseInfo_PropertyUseGroup',
    'API_PropertyUseInfo_YearBuilt',
    'API_ExtStructInfo_RoofMaterial',
    'API_ExtStructInfo_RoofConstruction',
    
    # Financial Information
    'API_CurrentDeed_MortgageAmount',
    'API_SaleInfo_DeedLastSalePrice',
    'API_Tax_AssessedValueTotal',
    
    # Property Features
    'API_IntRoomInfo_BathCount',
    'API_IntRoomInfo_BedroomsCount',
    'API_IntRoomInfo_RoomsCount',
    'API_PropertySize_AreaBuilding',
    'API_PropertySize_AreaLotSF'
]

# Keep only common columns that are in features_to_keep
common_features = list(set(positive_cases.columns) & set(negative_cases.columns) & set(features_to_keep))
common_features.append('roof_work_done')

print(f"\nNumber of selected features: {len(common_features)-1}")
print("\nSelected features:")
print([f for f in common_features if f != 'roof_work_done'])

# Keep only selected features
positive_cases = positive_cases[common_features]
negative_cases = negative_cases[common_features]

# Add target variable as boolean
positive_cases['roof_work_done'] = True
negative_cases['roof_work_done'] = False

# Balance the dataset
n_positive = len(positive_cases)
negative_cases_balanced = negative_cases.sample(n=n_positive, random_state=42)

print(f"\nNumber of positive cases: {len(positive_cases)}")
print(f"Number of negative cases (after balancing): {len(negative_cases_balanced)}")

# Combine datasets
df = pd.concat([positive_cases, negative_cases_balanced], axis=0)
df = df.reset_index(drop=True)

def prepare_features(df):
    # Separate numeric and categorical columns
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    categorical_cols = df.select_dtypes(include=['object']).columns
    
    # Remove target variable if present
    numeric_cols = [col for col in numeric_cols if col != 'roof_work_done']
    categorical_cols = [col for col in categorical_cols if col != 'roof_work_done']
    
    # Create a new DataFrame with reset index
    X = pd.DataFrame(index=df.index)
    
    # Handle numeric features
    for col in numeric_cols:
        if df[col].notna().any():
            median_val = df[col].median()
            X[col] = df[col].fillna(median_val)
    
    # Handle categorical features
    le = LabelEncoder()
    for col in categorical_cols:
        if df[col].notna().any():
            df[col] = df[col].fillna('Unknown')
            X[col] = le.fit_transform(df[col].astype(str))
    
    # Remove columns with all missing values or zero variance
    X = X.dropna(axis=1, how='all')
    
    return X, df['roof_work_done'], X.columns.tolist()

# Prepare features
print("\nPreparing features...")
X, y, feature_cols = prepare_features(df)

# First split: training vs (validation + test)
X_train, X_temp, y_train, y_temp = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Second split: validation vs test (50% each of the remaining 20%)
X_val, X_test, y_val, y_test = train_test_split(
    X_temp, y_temp, test_size=0.5, random_state=42, stratify=y_temp
)

print("\nData split sizes:")
print(f"Training set: {len(X_train)} ({len(X_train)/len(X)*100:.1f}%)")
print(f"Validation set: {len(X_val)} ({len(X_val)/len(X)*100:.1f}%)")
print(f"Test set: {len(X_test)} ({len(X_test)/len(X)*100:.1f}%)")

# Initialize model
model = XGBClassifier(
    learning_rate=0.05,
    n_estimators=200,
    max_depth=5,
    min_child_weight=3,
    gamma=0.2,
    subsample=0.8,
    colsample_bytree=0.8,
    objective='binary:logistic',
    random_state=42
)

# Train model
print("\nTraining model...")
model.fit(X_train, y_train)

# Also evaluate on validation set
val_score = model.score(X_val, y_val)
print(f"\nValidation accuracy: {val_score:.4f}")

# Function to evaluate model on a dataset
def evaluate_model(X, y, dataset_name):
    predictions = model.predict(X)
    pred_proba = model.predict_proba(X)[:, 1]
    
    print(f"\n=== {dataset_name} Set Performance ===")
    print("Classification Report:")
    print(classification_report(y, predictions))
    print(f"ROC AUC Score: {roc_auc_score(y, pred_proba):.4f}")
    
    # Plot confusion matrix
    plt.figure(figsize=(8, 6))
    cm = confusion_matrix(y, predictions)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
    plt.title(f'Confusion Matrix - {dataset_name} Set')
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    plt.show()
    
    return predictions, pred_proba

# Evaluate model on all sets
train_pred, train_proba = evaluate_model(X_train, y_train, "Training")
val_pred, val_proba = evaluate_model(X_val, y_val, "Validation")
test_pred, test_proba = evaluate_model(X_test, y_test, "Test")

# Plot feature importance
feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

plt.figure(figsize=(12, 8))
sns.barplot(data=feature_importance, x='importance', y='feature')
plt.title('Feature Importance')
plt.tight_layout()
plt.show()

# Save model and artifacts
model_artifacts = {
    'model': model,
    'feature_names': feature_cols,
    'feature_importance': feature_importance.to_dict(),
    'train_metrics': {
        'auc': roc_auc_score(y_train, train_proba),
        'predictions': train_pred,
        'probabilities': train_proba
    },
    'val_metrics': {
        'auc': roc_auc_score(y_val, val_proba),
        'predictions': val_pred,
        'probabilities': val_proba
    },
    'test_metrics': {
        'auc': roc_auc_score(y_test, test_proba),
        'predictions': test_pred,
        'probabilities': test_proba
    }
}

joblib.dump(model_artifacts, 'roofing_xgb_model_artifacts.joblib')
print("\nModel and artifacts saved successfully!")