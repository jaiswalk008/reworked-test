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

def calculate_roofing_betty_score(df):

    today = datetime.date.today()

    # Initialize 'BETTY SCORE' if it doesn't exist
    if 'BETTY SCORE' not in df.columns:
        df['BETTY SCORE'] = 0

    # Rule 1: Subtract 400 if the entity is public
    if 'is_public_entity' in df.columns:
        df.loc[df['is_public_entity'] == True, 'BETTY SCORE'] += 100

    # Rule 2: Subtract 150 if the owner full name is null
    if 'owner_full_name' in df.columns:
        df.loc[df['owner_full_name'].isnull(), 'BETTY SCORE'] -= 150

    # Rule 3: Subtract 80 if site mail zip distance is null
    if 'dist_btw_site_mail_zip' in df.columns:
        df.loc[df['dist_btw_site_mail_zip'].isnull(), 'BETTY SCORE'] -= 80

    # Rule 4: Subtract 70 if owner's last name is null
    if 'owner_last_name' in df.columns:
        df.loc[df['owner_last_name'].isnull(), 'BETTY SCORE'] -= 70

    # Rule 6: Subtract 80 if owner occupied is true
    if 'owner_occupied' in df.columns:
        df.loc[df['owner_occupied'] == True, 'BETTY SCORE'] -= 80

    # Rule 7: Subtract 30 for specific flood zone codes
    if 'flood_zone_code' in df.columns:
        df.loc[df['flood_zone_code'].str.strip().str.upper().isin(['A', 'AE', 'AH', 'AO', 'V', 'VE']), 'BETTY SCORE'] += 30

    if 'loan_to_value' in df.columns:
        df['loan_to_value'] = df['loan_to_value'].fillna(0).astype(str).apply(lambda x: re.sub(r'[^0-9.]', '', x)).astype(float)
        df.loc[df['loan_to_value'] <= 5, 'BETTY SCORE'] += 60
        df.loc[df['loan_to_value'] >= 90, 'BETTY SCORE'] += 120

    # Rule 14: Adjust based on last sale date
    if 'last_sale_date' in df.columns:
        df['last_sale_date'] = pd.to_datetime(df['last_sale_date'], errors='coerce')
        years_since_sale = today.year - df['last_sale_date'].dt.year
        df.loc[years_since_sale > 16, 'BETTY SCORE'] += 30
        df.loc[years_since_sale > 8, 'BETTY SCORE'] += 20
        df.loc[years_since_sale <= 3, 'BETTY SCORE'] -= 80

    # Rule 15: Adjust for market price and last sale price ratio
    if 'market_price' in df.columns and 'last_sale_price' in df.columns:
        df['price_ratio'] = df['market_price'] / df['last_sale_price']
        df.loc[df['price_ratio'] >= 1.5, 'BETTY SCORE'] += 70
        df.loc[df['price_ratio'] <= 1, 'BETTY SCORE'] -= 150

    # Rule 11: Adjust for potential_age
    if 'potential_age' in df.columns:
        df.loc[df['potential_age'] >= 45, 'BETTY SCORE'] += 60
        df.loc[df['potential_age'] < 45, 'BETTY SCORE'] -= 60

    if 'year_built' in df.columns:
        current_year = datetime.datetime.now().year
        df['age'] = current_year - df['year_built']  # Assuming current year is 2024
        # Apply the logic based on the age modulo 20
        df.loc[df['age'] % 20 >= 15, 'BETTY SCORE'] += 30
        df.loc[(df['age'] % 20 >= 10) & (df['age'] % 20 < 15), 'BETTY SCORE'] += 20
        df.loc[df['age'] % 20 < 10, 'BETTY SCORE'] += 5

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
    df = calculate_roofing_betty_score(df, industry_profile)
    if betty_scores_not_good(df):
      try:
        actual_ml_calling_count+=1
        df = actual_ml(df)
        df = recalc(df)
      except:
        pass
    tries = tries + 1
  return df, actual_ml_calling_count
