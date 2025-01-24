import pandas as pd
import numpy as np
from typing import Union, Dict, List

def classify_roofing_customer(customer_data: Union[pd.Series, Dict]) -> tuple[bool, Dict[str, bool]]:
    """
    Classify if a customer is a potential roofing customer based on defined rules.
    
    Args:
        customer_data: Either a pandas Series or dictionary containing customer information
        
    Returns:
        tuple: (is_qualified, detailed_results)
            - is_qualified: Boolean indicating if the customer meets all criteria
            - detailed_results: Dictionary with individual rule results
    """
    # Convert pandas Series to dict if needed
    if isinstance(customer_data, pd.Series):
        customer_data = customer_data.to_dict()
    
    # Initialize results dictionary
    results = {
        'meets_betty_score': False,
        'meets_roof_score': False,
        'meets_demo_score': False,
        'is_current_resident': False,
        'is_valid_entity': False
    }
    
    try:
        # Check BETTY Score (minimum 5)
        betty_score = float(customer_data.get('BETTY SCORE', 0))
        results['meets_betty_score'] = betty_score >= 5
        
        # Check BETTY_ROOF_SCORE (minimum 30)
        roof_score = float(customer_data.get('BETTY_ROOF_SCORE', 0))
        results['meets_roof_score'] = roof_score >= 30
        
        # Check BETTY_DEMOGRAPHIC_SCORE (minimum 20)
        demo_score = float(customer_data.get('BETTY_DEMOGRAPHIC_SCORE', 0))
        results['meets_demo_score'] = demo_score >= 20
        
        # Check if currently lives at address
        lives_at_address = customer_data.get('demo_currently_lives_in_address')
        results['is_current_resident'] = str(lives_at_address).lower() == 'true'
        
        # Check if not a business or public entity
        is_business = customer_data.get('is_business', False)
        is_public = customer_data.get('is_public_entity', False)
        results['is_valid_entity'] = not (is_business or is_public)
        
        # Final qualification requires meeting all criteria
        is_qualified = all(results.values())
        
        return is_qualified, results
        
    except Exception as e:
        print(f"Error processing customer data: {e}")
        return False, results

def batch_classify_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify multiple customers using the roofing criteria.
    
    Args:
        customers_df: Pandas DataFrame containing customer information
        
    Returns:
        pd.DataFrame: Original DataFrame with additional classification columns
    """
    # Create copy to avoid modifying original
    results_df = customers_df.copy()
    
    # Apply classification to each row
    classifications = []
    detailed_results = []
    
    for _, row in results_df.iterrows():
        is_qualified, details = classify_roofing_customer(row)
        classifications.append(is_qualified)
        detailed_results.append(details)
    
    # Add results to DataFrame
    results_df['is_qualified_prospect'] = classifications
    
    # Add individual rule results as separate columns
    for rule in detailed_results[0].keys():
        results_df[f'rule_{rule}'] = [result[rule] for result in detailed_results]
    
    return results_df

def get_classification_summary(results_df: pd.DataFrame) -> Dict:
    """
    Generate summary statistics for classification results.
    
    Args:
        results_df: DataFrame with classification results
        
    Returns:
        Dict: Summary statistics
    """
    total_records = len(results_df)
    qualified_records = results_df['is_qualified_prospect'].sum()
    
    summary = {
        'total_records': total_records,
        'qualified_prospects': qualified_records,
        'qualification_rate': qualified_records / total_records * 100,
        'rule_failure_rates': {}
    }
    
    # Calculate failure rates for each rule
    rule_columns = [col for col in results_df.columns if col.startswith('rule_')]
    for rule in rule_columns:
        failure_rate = (1 - results_df[rule].mean()) * 100
        summary['rule_failure_rates'][rule] = failure_rate
    
    return summary