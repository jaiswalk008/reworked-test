import pandas as pd
import datetime
import rei_ml
import re

def recalc(df):
  initialStepper = 10
  stride = 10
  maxSteppedValue = 150
  def bettyScoreLess():
    total_rows = df.shape[0]
    total_above_100 = df[df["BETTY SCORE"] > 100].count()["BETTY SCORE"]
    percent_above_100 = round((100/total_rows) * total_above_100, 2)
    if percent_above_100 < 30:
          return True
    return False
  def bettyScoreMore():
    total_rows = df.shape[0]
    total_above_100 = df[df["BETTY SCORE"] > 100].count()["BETTY SCORE"]
    percent_above_100 = round((100/total_rows) * total_above_100, 2)
    if percent_above_100 >75:
          return True
    return False
  # countOfDNC = len(df.loc[df['Predicted'] == 'DO_NOT_CONTACT'])/len(df)
  # if countOfDNC < 0.25:
  #   return df
  currentReducer = 0
  currentIncreamenter = 0
  loopCounter = 0
  while betty_scores_not_good(df):
    if bettyScoreMore() and (not loopCounter):
      df.loc[(df['Predicted'] == 'DO_NOT_CONTACT') & (df['BETTY SCORE'] >= 100) , 'BETTY SCORE'] = df['BETTY SCORE'] - initialStepper
    elif bettyScoreMore() and currentReducer <= maxSteppedValue:
      df.loc[(df['Predicted'] == 'DO_NOT_CONTACT') & (df['BETTY SCORE'] >= 100) , 'BETTY SCORE'] = df['BETTY SCORE'] - stride
      currentReducer += stride
    elif bettyScoreLess() and (not loopCounter):
      df.loc[(df['Predicted'] == 'CONTACT') & (df['BETTY SCORE'] < 100) , 'BETTY SCORE'] = df['BETTY SCORE'] + initialStepper
    elif bettyScoreLess() and currentIncreamenter <= maxSteppedValue:
      df.loc[(df['Predicted'] == 'CONTACT') & (df['BETTY SCORE'] >= 100) , 'BETTY SCORE'] = df['BETTY SCORE'] + stride
      currentIncreamenter += stride
    else:
      break
    loopCounter += 1
  # BETTY SCORE = BETTY SCORE; Predicted = Predicted
  return df

def betty_scores_not_good(df):
  total_rows = df.shape[0]
  total_above_100 = df[df["BETTY SCORE"] > 100].count()["BETTY SCORE"]
  percent_above_100 = round((100/total_rows) * total_above_100, 2)
  # Betty scores aren't good if majority (more than 75%) of the rows have score above 100
  if  percent_above_100 > 75 or percent_above_100 < 30: 
    return True
  # False means betty scores are good, i.e. majority of the rows are below 100
  else:
    return False


def rules_ml(df, industry_profile): 

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Generic Processing, irrespective of channel, irrespective of the type of property they're after
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    if 'is_public_entity' in df.columns:
      df.loc[df['is_public_entity']==True , 'BETTY SCORE'] = df['BETTY SCORE'] - 400

    df.loc[df['owner_full_name'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 150
    df.loc[df['dist_btw_site_mail_zip'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 80

    if 'owner_last_name' in df.columns:
      df.loc[df['owner_last_name'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 70

    if 'estimated_equity' in df.columns:
      df.loc[df['estimated_equity'] < 0, 'BETTY SCORE'] = df['BETTY SCORE'] + 120
    
    #TODO: Is this right?
    if 'owner_occupied' in df.columns:
      df.loc[df['owner_occupied']==True, 'BETTY SCORE'] = df['BETTY SCORE'] - 80
        
    if 'flood_zone_code' in df.columns:
      try:
        df.loc[df['flood_zone_code'].str.strip().upper().isin(['A', 'AE', 'AH', 'AO', 'V', 'VE']) , 'BETTY SCORE'] = df['BETTY SCORE'] - 30
      except: 
        pass
    
    today = datetime.date.today()

    if 'demo_date_of_birth' in df.columns:
      df.loc[(today.year - pd.to_datetime(df['demo_date_of_birth']).dt.year) > 60, 'BETTY SCORE'] = df['BETTY SCORE'] + 95
      
    if 'date_of_death' in df.columns:
      if 'single' in industry_profile['property_type']:
        df.loc[df['date_of_death'].notnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 95
      else:
        df.loc[df['date_of_death'].notnull(), 'BETTY SCORE'] = df['BETTY SCORE'] + 95

    age_column = 'potential_age'
    if 'age' in df.columns:
      age_column = 'age' 
    df.loc[(df[age_column] > 60), 'BETTY SCORE'] = df['BETTY SCORE'] + 95
    df.loc[(df[age_column] > 40) & (df[age_column] < 60), 'BETTY SCORE'] = df['BETTY SCORE'] + 35
    
    if 'deceased' in df.columns:
      df.loc[df['deceased'].str.lower() == "y", 'BETTY SCORE'] = df['BETTY SCORE'] + 95
    
    df.loc[(df['dist_btw_site_mail_zip'] > 70) & (df['dist_btw_site_mail_zip'] < 250) , 'BETTY SCORE'] = df['BETTY SCORE'] + 70
    if 'single' not in industry_profile['property_type']:
      df.loc[df['dist_btw_site_mail_zip'] < 30, 'BETTY SCORE'] = df['BETTY SCORE'] - 70
    df.loc[df['dist_btw_site_mail_zip'] >= 250, 'BETTY SCORE'] = df['BETTY SCORE'] + 120
    
    if 'single' in industry_profile['property_type']:
      if 'is_address_same' in df.columns:
        df.loc[df['is_address_same']==True, 'BETTY SCORE'] = df['BETTY SCORE'] - 60
        
    if 'equity_percent' in df.columns:
      df.loc[df['equity_percent'] >= 90, 'BETTY SCORE'] = df['BETTY SCORE'] + 60
      df.loc[df['equity_percent'] <= 5, 'BETTY SCORE'] = df['BETTY SCORE'] + 120
    elif 'loan_to_value' in df.columns:
      df['loan_to_value'] = df['loan_to_value'].fillna(0)
      df['loan_to_value'] = df['loan_to_value'].apply(lambda x: re.sub(r'[^0-9.]', '', str(x))).astype(float)
      df.loc[df['loan_to_value'] <= 5, 'BETTY SCORE'] = df['BETTY SCORE'] + 60
      df.loc[df['loan_to_value'] >= 90, 'BETTY SCORE'] = df['BETTY SCORE'] + 120
    
    if 'absentee_owner' in df.columns:
       df.loc[df['absentee_owner'].str.lower()=='yes' , 'BETTY SCORE'] = df['BETTY SCORE'] + 100

    if 'last_sale_document_type' in df.columns:
      df.loc[df['last_sale_document_type'].str.lower() == "intrafamily transfer & dissolution" , 'BETTY SCORE'] = df['BETTY SCORE'] + 60

    if 'last_sale_date' in df.columns:
      df.loc[(today.year - pd.to_datetime(df['last_sale_date'], errors='coerce',infer_datetime_format=True ).dt.year) > 8, 'BETTY SCORE'] = df['BETTY SCORE'] + 20
      df.loc[(today.year - pd.to_datetime(df['last_sale_date'], errors='coerce',infer_datetime_format=True).dt.year) > 16, 'BETTY SCORE'] = df['BETTY SCORE'] + 30
      df.loc[(today.year - pd.to_datetime(df['last_sale_date'], errors='coerce',infer_datetime_format=True).dt.year) <= 3, 'BETTY SCORE'] = df['BETTY SCORE'] - 80

    if 'market_price' in df.columns and 'last_sale_price' in df.columns:
      df.loc[df['market_price'] / df['last_sale_price'] >= 1.5, 'BETTY SCORE'] = df['BETTY SCORE'] + 70
      df.loc[df['market_price'] / df['last_sale_price'] <= 1, 'BETTY SCORE'] = df['BETTY SCORE'] - 150

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Generic Processing, mail 
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    if "mail" in industry_profile['marketing_campaign']:

      if 'BETTY_UPDATED_ADDRESS_LINE1' in df.columns:
        df.loc[df['BETTY_UPDATED_ADDRESS_LINE1'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 300
      
      if 'do_not_mail' in df.columns:
       df.loc[df['do_not_mail']==True , 'BETTY SCORE'] = df['BETTY SCORE'] - 300

      if 'demo_address_verification_failed' in df.columns:
        df.loc[df['demo_address_verification_failed'] == True , 'BETTY SCORE'] = df['BETTY SCORE'] - 350
      
      if 'owner_mailing_name' in df.columns:
       df.loc[df['owner_mailing_name'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 60

       if 'mail_zip_code' in df.columns:
        df.loc[df['mail_zip_code'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 100
      
      #if 'demo_currently_lives_in_address' in df.columns:
      #  df.loc[df['demo_currently_lives_in_address'] == False , 'BETTY SCORE'] = df['BETTY SCORE'] - 75

   # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # Generic Processing, sms and/or robo-call
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    if any(x in industry_profile['marketing_campaign'] for x in ['sms', 'call-live', 'call-robo']):
      if 'do_not_contact' in df.columns:
       df.loc[df['do_not_contact']=="DNC" , 'BETTY SCORE'] = df['BETTY SCORE'] - 300
      
      if 'mobile_number_present' in df.columns:
        df.loc[df['mobile_number_present']==False, 'BETTY SCORE'] = df['BETTY SCORE'] - 200
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # Land Processing
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    if "land" in industry_profile['property_type']:
      df.loc[df['apn'].isnull(), 'BETTY SCORE'] = df['BETTY SCORE'] - 60

      if 'lot_area_sqft' in df.columns:
        df.loc[df['lot_area_sqft'] < 4500, 'BETTY SCORE'] = df['BETTY SCORE'] - 40

      #This was added after a user faced issue where the mailing address was missing in the county records for some of the records, so the property address and mailing
      # address was same, of course you can't mail to an empty plot so all mail came back as undeliverable 
      if 'is_address_same' in df.columns:
        df.loc[df['is_address_same']==True, 'BETTY SCORE'] = df['BETTY SCORE'] - 180

    # The below was used for Maya when she needed homes to be processed and not vacant land
    #if 'property_type' in df.columns:
    #  df.loc[df['property_type'].str.strip().isin(['Vacant Land (General)', 'Residential-Vacant Land', 'Industrial (General)']), 'BETTY SCORE'] = df['BETTY SCORE'] - 250


    if 'year_renovated' in df.columns:
      # If a property was renovated in the last 5 years then chances are less that they'll sell
      df.loc[df['year_renovated'] > (today.year -5), 'BETTY SCORE'] = df['BETTY SCORE'] - 80

    if 'year_built' in df.columns:
      # If a property was built recently then chances are less of a sale
      df.loc[df['year_built'] > (today.year -5), 'BETTY SCORE'] = df['BETTY SCORE'] - 80

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # SingleFamily Processing
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    if 'for_sale' in df.columns:
      df.loc[df['for_sale'] == 'Yes' , 'BETTY SCORE'] = df['BETTY SCORE'] - 220
    
    if 'inherited' in df.columns:
      df.loc[df['inherited'] == 'Yes' , 'BETTY SCORE'] = df['BETTY SCORE'] + 90

    if "single" in industry_profile['property_type']:
      if 'property_type' in df.columns:
        mode_sfr = df['property_type'].mode()
        if len(mode_sfr) > 1:
          df.loc[df['property_type'] != mode_sfr[0] , 'BETTY SCORE'] = df['BETTY SCORE'] - 200

    if 'num_owners' in df.columns:
      df.loc[df['num_owners'] > 1 , 'BETTY SCORE'] = df['BETTY SCORE'] - 20

    if 'property_vacant' in df.columns:
      mode_property_vacant = df['property_vacant'].mode()
      if len(mode_property_vacant) > 1:
        df.loc[df['property_vacant'] != mode_property_vacant[0] , 'BETTY SCORE'] = df['BETTY SCORE'] + 60

    if 'property_vacant' in df.columns and 'owner_num_total_properties' in df.columns:
      df.loc[(df['property_vacant']== True) & (df['owner_num_total_properties']>1), 'BETTY SCORE'] = df['BETTY SCORE'] + 80
      df.loc[(df['property_vacant']== False) & (df['owner_num_total_properties']== 1), 'BETTY SCORE'] = df['BETTY SCORE'] - 30
      
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Multifamily Processing
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
    # Commercial Processing
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    if 'vacancy_percent' in df.columns:
      median_vacancy = df['vacancy_percent'].median()
      df.loc[df['vacancy_percent'] > median_vacancy , 'BETTY SCORE'] = df['BETTY SCORE'] + 90

    if 'star_rating' in df.columns:
      df.loc[df['vacancy_percent'] > 3 , 'BETTY SCORE'] = df['BETTY SCORE'] - 70
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
    # NOT Multifamily OR Commercial Processing
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    
    if any(x in industry_profile['property_type'] for x in ['land', 'single']):
      if 'owner_1_type' in df.columns:
        df.loc[df['owner_1_type'] ==1 , 'BETTY SCORE'] = df['BETTY SCORE'] + 50 
  
      df.loc[df['is_business']==True , 'BETTY SCORE'] = df['BETTY SCORE'] - 60
    

    #if 'delinquent_tax_value' in df.columns:
    #  df.loc[df['delinquent_tax_value'] / df['property_tax'] < 1.0, 'BETTY SCORE'] = df['BETTY SCORE'] - 50

    if 'Predicted' in df.columns:
      df = recalc(df)
    return df

def actual_ml(df):
  if 'Predicted' in df.columns:
    df = df.drop('Predicted', axis=1)
  df = rei_ml.process_ml(df)
  return df

def compute_betty(df, industry_profile):
  df['BETTY SCORE'] = 100
  actual_ml_calling_count = 0
  tries = 0
  while (tries < 1) and (betty_scores_not_good(df)):
    df['BETTY SCORE'] = 100
    df = rules_ml(df, industry_profile)
    if betty_scores_not_good(df):
      try:
        actual_ml_calling_count+=1
        df = actual_ml(df)
        df = recalc(df)
      except:
        pass
    tries = tries + 1
  return df, actual_ml_calling_count
