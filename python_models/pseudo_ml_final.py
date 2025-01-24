import pandas as pd
import sys
import pgeocode as pgeo
import logging
import json
import modules.rules_engine as rules_engine

def calculate_confidence_score(df, actual_ml_calling_count):
  confidence_dict = {}
  confidence_score = 100
  
  confidence_dict['confidence_score'] = confidence_score
  # Condition 1: if actual_ml kicked in then reduce score by 10
  confidence_dict['actual_ml_calling_count'] = actual_ml_calling_count
  confidence_score = confidence_score - (10 * actual_ml_calling_count)
  confidence_dict['actual_ml_score'] = confidence_score
  
  # Calculate percentages for each condition
  full_name_missing_percentage = df['full_name_missing'].mean() * 100
  dist_btw_site_mail_zip_percentage = ((df['dist_btw_site_mail_zip'] == 9999.0) | (df['dist_btw_site_mail_zip'].isna())).mean() * 100
  potential_age_above_threshold_percentage = (df['age_source'] == 'Melissa').mean() * 100
  demo_address_verification_failed_percentage = df['demo_address_verification_failed'].mean() * 100
  demo_currently_lives_in_address_percentage = (df['demo_currently_lives_in_address'] == False).mean() * 100

  # Condition 2: 'full_name_missing' is TRUE for more than 30%
  confidence_dict['full_name_missing_percentage'] = round(full_name_missing_percentage)
  if full_name_missing_percentage > 30:
    reduction = 50
    confidence_score -= reduction
    confidence_dict['full_name_missing_score'] = round(confidence_score)
    
  # Condition 3: 'dist_btw_site_mail_zip' is 9999.0 or Nan for more than 30%
  confidence_dict['dist_btw_site_mail_zip_percentage'] = round(dist_btw_site_mail_zip_percentage)
  if dist_btw_site_mail_zip_percentage > 30:
    reduction = 30
    confidence_score -= reduction
    confidence_dict['confidence_score_after_dist_btw_site_mail_zip'] = round(confidence_score)

  # Condition 4: 'potential_age' is filled with mode(potential_age) for more than 40%
  confidence_dict['potential_age_above_threshold_percentage'] = round(potential_age_above_threshold_percentage)
  if potential_age_above_threshold_percentage < 40:
    reduction = 50
    confidence_score -= reduction
    confidence_dict['confidence_score_after_potential_age_above_threshold'] = round(confidence_score)

  # Condition 5: 'demo_address_verification_failed' is TRUE for more than 50%
  confidence_dict['demo_address_verification_failed_percentage'] = round(demo_address_verification_failed_percentage)
  if demo_address_verification_failed_percentage > 50:
      reduction = 50
      confidence_score -= reduction
      confidence_dict['confidence_score_after_demo_address_verification_failed'] = round(confidence_score)

  # Condition 6: 'demo_currently_lives_in_address' is FALSE for more than 50%
  confidence_dict['demo_currently_lives_in_address_percentage'] = round(demo_currently_lives_in_address_percentage)
  if demo_currently_lives_in_address_percentage > 50:
      reduction = 50
      confidence_score -= reduction
      confidence_dict['confidence_score_after_demo_currently_lives_in_address'] = round(confidence_score)
  confidence_dict['final_confidence_score'] = round(confidence_score)
  return confidence_dict
  
  
def create_predicted(row):
  if row['BETTY SCORE'] > 100:
    return 'CONTACT'
  else:
    return 'DO_NOT_CONTACT'

logging.basicConfig(level=logging.INFO, format="%(message)s")
try:
  industry_profile = {}

  for i,arg in enumerate(sys.argv):
      if arg == '--file_path':
          file_path = sys.argv[i+1]
      if arg == "--industry_profile":
          industry_profile = json.loads(sys.argv[i+1])
  new_file_path = file_path.split('.csv')[0]+'_betty.csv'
  df = pd.read_csv(file_path, low_memory=False, on_bad_lines='warn')

  df, actual_ml_calling_count = rules_engine.compute_betty(df, industry_profile)
  confidence_dict = calculate_confidence_score(df, actual_ml_calling_count)
  if not 'Predicted' in df.columns:
    df['Predicted'] = df.apply(lambda row: create_predicted(row), axis=1)
  betty= df.pop('BETTY SCORE')
  df = df.assign(betty_score=betty)
  df.rename(columns={'betty_score': 'BETTY SCORE'}, inplace=True)  
  df.to_csv(new_file_path, index=False)
  logging.info({"success":"True","OriginalFilePath": file_path,"NewFilePath": new_file_path, "ConfidenceDict": confidence_dict})
except Exception as e:
  exc = str(e).replace('"','')
  exc = str(exc).replace("'",'')
  logging.error({"success":"False","OriginalFilePath": file_path,"NewFilePath": new_file_path, "error": "betty_failure", "error_details": exc})
