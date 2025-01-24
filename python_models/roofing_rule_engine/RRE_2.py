import pandas as pd
from typing import Dict, Tuple, Any
from datetime import datetime

# Define optimal values and ranges for all columns
OPTIMAL_RANGES = {
    'API_PropertyUseInfo_YearBuilt': {'optimal': 1985, 'range': [1960, 2010]},
    'API_PropertyUseInfo_PropertyUseMuni': {'optimal': 'Single Family', 'range': ['Single Family', 'Residential']},
    'API_PropertyUseInfo_PropertyUseGroup': {'optimal': 'Residential', 'range': ['Residential', 'Single Family']},
    'API_PropertySize_AreaBuilding': {'optimal': 2200, 'range': [1500, 3500]},
    'API_PropertySize_AreaLotSF': {'optimal': 8500, 'range': [5000, 15000]},
    'API_IntRoomInfo_StoriesCount': {'optimal': 2, 'range': [1, 3]},
    'API_IntRoomInfo_BedroomsCount': {'optimal': 3, 'range': [2, 4]},
    'API_Tax_MarketValueTotal': {'optimal': 350000, 'range': [250000, 550000]},
    'API_EstimatedValue_EstimatedValue': {'optimal': 375000, 'range': [275000, 575000]},
    'API_Tax_AssessedValueTotal': {'optimal': 280000, 'range': [200000, 450000]},
    'API_PropertyAddress_CarrierRoute': {'optimal': 'Suburban', 'range': ['Suburban', 'Urban']},
    'API_ExtStructInfo_PlumbingFixturesCount': {'optimal': 8, 'range': [6, 12]},
    'API_IntStructInfo_Construction': {'optimal': 'Standard', 'range': ['Standard', 'Custom']},
    'API_PropertySize_BasementArea': {'optimal': 1000, 'range': [500, 2000]},
    'API_ExtAmenities_PorchArea': {'optimal': 150, 'range': [50, 300]},
    'API_YardGardenInfo_FenceArea': {'optimal': 200, 'range': [100, 400]},
    'API_ExtBuildings_BuildingsCount': {'optimal': 1, 'range': [0, 3]},
    'API_LastDeedOwnerInfo_OwnerOccupied': {'optimal': 'Yes', 'range': ['Yes']},
    'API_Tax_TaxBilledAmount': {'optimal': 4500, 'range': [3000, 7000]},
    'API_EstimatedValue_ConfidenceScore': {'optimal': 0.85, 'range': [0.7, 1.0]},
    'API_PropertySize_BasementAreaFinished': {'optimal': 800, 'range': [400, 1600]},
    'API_PropertySize_BasementAreaUnfinished': {'optimal': 200, 'range': [0, 600]},
    'API_PropertySize_ParkingGarageArea': {'optimal': 400, 'range': [200, 800]},
    'API_IntRoomInfo_BathCount': {'optimal': 2.5, 'range': [1.5, 3.5]},
    'API_IntRoomInfo_BathPartialCount': {'optimal': 1, 'range': [0, 2]},
    'API_IntRoomInfo_RoomsCount': {'optimal': 7, 'range': [5, 10]},
    'API_IntRoomInfo_UnitsCount': {'optimal': 1, 'range': [1, 2]},
    'API_Tax_AssessedValueImprovements': {'optimal': 200000, 'range': [150000, 350000]},
    'API_Tax_AssessedValueLand': {'optimal': 80000, 'range': [50000, 150000]},
    'API_Tax_MarketValueImprovements': {'optimal': 220000, 'range': [160000, 380000]},
    'API_Tax_MarketValueLand': {'optimal': 90000, 'range': [60000, 160000]},
    'API_PropertySize_Area1stFloor': {'optimal': 1400, 'range': [1000, 2200]},
    'API_PropertySize_Area2ndFloor': {'optimal': 1000, 'range': [600, 1800]},
    'API_PropertySize_AreaUpperFloors': {'optimal': 800, 'range': [400, 1600]},
    'API_PropertySize_AtticArea': {'optimal': 600, 'range': [200, 1200]},
    'API_Parking_ParkingSpaceCount': {'optimal': 2, 'range': [1, 4]},
    'API_Parking_DrivewayArea': {'optimal': 600, 'range': [300, 1200]},
    'API_IntAmenities_FireplaceCount': {'optimal': 1, 'range': [0, 2]},
    'API_ExtBuildings_ShedArea': {'optimal': 120, 'range': [80, 200]},
    'API_ExtBuildings_GarageArea': {'optimal': 440, 'range': [300, 800]},
    'API_Tax_TaxFiscalYear': {'optimal': 2023, 'range': [2022, 2024]},
    'API_PropertySize_LotDepth': {'optimal': 120, 'range': [80, 200]},
    'API_PropertySize_LotWidth': {'optimal': 60, 'range': [40, 100]}
}

def calculate_home_age_score(year_built: int) -> Tuple[int, str]:
    """Calculate score based on home age and its modulo 22 pattern."""
    current_year = datetime.now().year
    home_age = current_year - year_built
    modulo_22 = home_age % 22
    
    if home_age <= 12:
        return -80, "Home too new, less likely to need roofing"
    elif modulo_22 > 18:
        return 50, "Prime age for roofing work (modulo > 18)"
    elif 12 <= modulo_22 <= 18:
        return 30, "Good potential for roofing work (modulo 12-18)"
    elif home_age > 22 and 0 <= modulo_22 <= 5:
        return 50, "Recently past maintenance cycle"
    return 0, "Standard age profile"

def evaluate_numeric_field(value: float, field_name: str) -> Tuple[int, str]:
    """Evaluate a numeric field against its optimal range."""
    if field_name not in OPTIMAL_RANGES:
        return 0, "Field not in optimal ranges"
    
    # Try to convert value to float if it's a string
    try:
        value = float(value) if isinstance(value, str) else value
    except (ValueError, TypeError):
        return 0, f"Invalid numeric value for {field_name}"
    
    optimal = OPTIMAL_RANGES[field_name]['optimal']
    range_min, range_max = OPTIMAL_RANGES[field_name]['range']
    
    if value == optimal:
        return 30, f"Optimal value for {field_name}"
    elif range_min <= value <= range_max:
        return 20, f"Within acceptable range for {field_name}"
    return 0, f"Outside optimal range for {field_name}"

def evaluate_categorical_field(value: str, field_name: str) -> Tuple[int, str]:
    """Evaluate a categorical field against its acceptable values."""
    if field_name not in OPTIMAL_RANGES:
        return 0, "Field not in optimal ranges"
    
    optimal = OPTIMAL_RANGES[field_name]['optimal']
    acceptable_values = OPTIMAL_RANGES[field_name]['range']
    
    if value == optimal:
        return 30, f"Optimal value for {field_name}"
    elif value in acceptable_values:
        return 20, f"Acceptable value for {field_name}"
    return 0, f"Non-optimal value for {field_name}"

def classify_roofing_customer(customer_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Classify a potential roofing customer based on all available criteria.
    """
    score = 0
    details = {}
    
    # Critical Disqualifiers
    if customer_data.get('is_public_entity', False):
        return False, {'public_entity': (-100, "Public entity - disqualified")}
    
    if customer_data.get('demo_address_verification_failed', False):
        return False, {'address_verification': (-100, "Failed address verification")}
        
    if customer_data.get('is_apartment', False):
        return False, {'property_type': (-200, "Apartment - not suitable")}
    
    if customer_data.get('API_PropertyUseInfo_PropertyUseStandardized') not in [385, 386]:
        return False, {'property_use': (-90, "Non-standard property use")}
    
    # Property Features Scoring
    for field, value in customer_data.items():
        if field in OPTIMAL_RANGES:
            # Try to convert string numbers to float for numeric fields
            try:
                if isinstance(value, str) and any(c.isdigit() for c in value):
                    value = float(value.replace(',', ''))
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.','',1).isdigit()):
                    field_score, explanation = evaluate_numeric_field(value, field)
                else:
                    field_score, explanation = evaluate_categorical_field(str(value), field)
                score += field_score
                details[field] = (field_score, explanation)
            except (ValueError, TypeError):
                field_score, explanation = evaluate_categorical_field(str(value), field)
                score += field_score
                details[field] = (field_score, explanation)
    
    # Special Home Age Scoring
    year_built = customer_data.get('API_PropertyUseInfo_YearBuilt')
    if year_built:
        age_score, age_explanation = calculate_home_age_score(year_built)
        score += age_score
        details['home_age_special'] = (age_score, age_explanation)
    
    # Demographic and Ownership Scoring
    if customer_data.get('ownrent') == 4:  # Definite Owner
        score += 30
        details['ownership'] = (30, "Definite owner")
    
    if customer_data.get('demo_currently_lives_in_address', True):
        score += 20
        details['residency'] = (20, "Current resident")
    
    # Age Scoring
    age = customer_data.get('age')
    if age:
        if 45 <= age <= 55:
            score += 10
            details['age'] = (10, "Age group 45-55")
        elif age >= 65:
            score += 10
            details['age'] = (10, "Age group 65+")
        elif 55 <= age <= 65:
            score += 30
            details['age'] = (30, "Optimal age group 55-65")
    
    # Additional Demographic Factors
    if customer_data.get('presenceofchildren', False):
        score += 20
        details['children'] = (20, "Presence of children")
    
    if 7 <= customer_data.get('householdincome', 0) <= 9:
        score += 30
        details['income'] = (30, "Optimal income range")
    
    if 2 <= customer_data.get('householdsize', 0) <= 5:
        score += 30
        details['household_size'] = (30, "Optimal household size")
    
    # Residence Length
    residence_length = customer_data.get('lengthofresidence', 0)
    if 5.5 <= residence_length <= 10.5:
        score += 20
        details['residence_length'] = (20, "Optimal length of residence")
    
    # Negative Factors
    if customer_data.get('full_name_missing', False):
        score -= 20
        details['name_complete'] = (-20, "Missing full name")
    
    if customer_data.get('is_business', False):
        score -= 20
        details['business'] = (-20, "Business entity")
    
    if customer_data.get('is_po_box', False):
        score -= 30
        details['address_type'] = (-30, "PO Box address")
    
    # Final Classification
    is_qualified = score >= 100  # Threshold for qualification
    
    details['total_score'] = (score, "Total qualification score")
    details['is_qualified'] = (is_qualified, f"{'Qualified' if is_qualified else 'Not qualified'} prospect")
    
    return is_qualified, details

def batch_classify_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Classify multiple customers using the roofing criteria."""
    results_df = customers_df.copy()
    classifications = []
    detailed_results = []
    
    for _, row in results_df.iterrows():
        is_qualified, details = classify_roofing_customer(row.to_dict())
        classifications.append(is_qualified)
        detailed_results.append(details)
    
    # Add results to DataFrame
    results_df['is_qualified_prospect'] = classifications
    results_df['qualification_score'] = [result.get('total_score', (0, ""))[0] for result in detailed_results]
    
    # Add individual rule results as separate columns
    rule_keys = detailed_results[0].keys()
    for rule in rule_keys:
        if rule != 'total_score' and rule != 'is_qualified':
            results_df[f'rule_{rule}'] = [result.get(rule, (0, ""))[0] for result in detailed_results]
            results_df[f'rule_{rule}_explanation'] = [result.get(rule, (0, ""))[1] for result in detailed_results]
    
    return results_df

def get_classification_summary(results_df: pd.DataFrame) -> Dict:
    """Generate summary statistics for classification results."""
    total_records = len(results_df)
    qualified_records = results_df['is_qualified_prospect'].sum()
    
    summary = {
        'total_records': total_records,
        'qualified_prospects': int(qualified_records),
        'qualification_rate': round(qualified_records / total_records * 100, 2),
        'average_score': round(results_df['qualification_score'].mean(), 2),
        'score_distribution': {
            'min': int(results_df['qualification_score'].min()),
            'max': int(results_df['qualification_score'].max()),
            'median': int(results_df['qualification_score'].median())
        },
        'rule_failure_rates': {}
    }
    
    # Calculate failure rates for each rule
    rule_columns = [col for col in results_df.columns if col.startswith('rule_') 
                   and not col.endswith('_explanation')]
    
    for rule in rule_columns:
        if results_df[rule].dtype in ['int64', 'float64']:
            failure_rate = (results_df[rule] <= 0).mean() * 100
            summary['rule_failure_rates'][rule] = round(failure_rate, 2)
    
    return summary