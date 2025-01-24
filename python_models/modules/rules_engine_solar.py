import pandas as pd
import datetime

def score_demo(df): 
    today = datetime.date.today()

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df['demo_gender'].dtypes)
        print(df['demo_gender'].value_counts())

    age_column = 'potential_age'
    if 'age' in df.columns:
      age_column = 'age' 
    df.loc[df[age_column].isnull(), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10
    df.loc[(df[age_column] >= 37) & (df[age_column] <= 53), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 30
    df.loc[(df[age_column] > 53) & (df[age_column] <= 72), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 20
    df.loc[(df[age_column] >= 27) & (df[age_column] < 37), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10
    df.loc[(df[age_column] > 72), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10

    df.loc[df['ownrent']=='Definite Owner' , 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 30

    df.loc[df['politicalparty']=='Democrat' , 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10

    df.loc[df['maritalstatus']=='Definitely Married' , 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10
    df.loc[df['maritalstatus']=='Definitely Single' , 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10

    df.loc[df['demo_currently_lives_in_address'] == True , 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10

    df.loc[((df['demo_gender'] == 'M') | (df['demo_gender'] == 'unkownn' )), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 20

    df.loc[(df['education'] == 'Graduate School'), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10

    df.loc[(df['presenceofchildren'] == 'Children Present'), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10


    df.loc[((df['lengthofresidence'] == '15+') | (df['lengthofresidence'] == 'Less than 1 year') ), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] - 10
    df.loc[((df['lengthofresidence'] == '3-4') 
            | (df['lengthofresidence'] == '4-5')
             | (df['lengthofresidence'] == '2-3') 
              | (df['lengthofresidence'] == '5-6')
               | (df['lengthofresidence'] == '6-7')), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 20

    df.loc[((df['lengthofresidence'] == '1-2') 
            | (df['lengthofresidence'] == '7-8')
             | (df['lengthofresidence'] == '9-10')), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10

    df.loc[((df['householdsize'] == 2) | (df['householdsize'] == 3) ), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 20
    df.loc[((df['householdsize'] == 1) | (df['householdsize'] == 4) ), 'BETTY_DEMOGRAPHIC_SCORE'] = df['BETTY_DEMOGRAPHIC_SCORE'] + 10


    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Zero'ing out of score
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    df.loc[df['ownrent']=='Definite Renter' , 'BETTY_DEMOGRAPHIC_SCORE'] = 0

    if 'is_business' in df.columns:
      df.loc[df['is_business']==True , 'BETTY_DEMOGRAPHIC_SCORE'] = 0

    if 'is_public_entity' in df.columns:
      df.loc[df['is_public_entity']==True , 'BETTY_DEMOGRAPHIC_SCORE'] = 0

    df.loc[df['owner_full_name'].isnull(), 'BETTY_DEMOGRAPHIC_SCORE'] = 0
    
    if 'do_not_mail' in df.columns:
      df.loc[df['do_not_mail']==True , 'BETTY_DEMOGRAPHIC_SCORE'] = 0

    if 'demo_address_verification_failed' in df.columns:
        df.loc[df['demo_address_verification_failed'] == True , 'BETTY_DEMOGRAPHIC_SCORE'] = 0
  
    if 'do_not_contact' in df.columns:
        df.loc[df['do_not_contact']=="DNC" , 'BETTY_DEMOGRAPHIC_SCORE'] = 0    
        
    return df


def score_roof(df):
    df.loc[(df['solarPotential.maxArrayPanelsCount'] >= 48) & (df['solarPotential.maxArrayPanelsCount'] <= 97) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 30
    df.loc[(df['solarPotential.maxArrayPanelsCount'] > 7) & (df['solarPotential.maxArrayPanelsCount'] < 48) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 20
    df.loc[(df['solarPotential.maxArrayPanelsCount'] > 97) & (df['solarPotential.maxArrayPanelsCount'] <= 243) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 20
    df.loc[(df['solarPotential.maxArrayPanelsCount'] > 243) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 10
    
    df.loc[(df['solarPotential.maxSunshineHoursPerYear'] >= 1500) & (df['solarPotential.maxSunshineHoursPerYear'] <= 1700) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 30
    df.loc[(df['solarPotential.maxSunshineHoursPerYear'] >= 800) & (df['solarPotential.maxSunshineHoursPerYear'] < 1500) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 20
    df.loc[(df['solarPotential.maxSunshineHoursPerYear'] > 1700) & (df['solarPotential.maxSunshineHoursPerYear'] <= 2050) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 20
    df.loc[(df['solarPotential.maxSunshineHoursPerYear'] > 2050) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 10
    
    df.loc[(df['solarPotential.roofSegmentStats'] >= 4) & (df['solarPotential.roofSegmentStats'] <= 13) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 30
    df.loc[(df['solarPotential.roofSegmentStats'] >= 1) & (df['solarPotential.roofSegmentStats'] < 4) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 20
    df.loc[(df['solarPotential.roofSegmentStats'] > 13) & (df['solarPotential.roofSegmentStats'] <= 30) , 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 20
    df.loc[(df['solarPotential.roofSegmentStats'] > 30) & (df['solarPotential.roofSegmentStats'] <= 60), 'BETTY_ROOF_SCORE'] = df['BETTY_ROOF_SCORE'] + 10

    df.loc[(df['is_apartment'] == True), 'BETTY_ROOF_SCORE'] = 0 

    return df


def rule_engine_solar(df):
    df['BETTY_ROOF_SCORE'] = 0
    df['BETTY_DEMOGRAPHIC_SCORE'] = 0
    df = score_roof(df)
    df_with_scores = score_demo(df)
    df_with_scores['BETTY SCORE'] = 0
    df_with_scores['BETTY SCORE'] = 0.7*df_with_scores['BETTY_DEMOGRAPHIC_SCORE'].astype(float) + 0.3*df_with_scores['BETTY_ROOF_SCORE'].astype(float)
    df_with_scores['BETTY_ROOF_SCORE'] = df_with_scores['BETTY_ROOF_SCORE'].apply(lambda x: round(x, 2))
    df_with_scores['BETTY_DEMOGRAPHIC_SCORE'] = df_with_scores['BETTY_DEMOGRAPHIC_SCORE'].apply(lambda x: round(x, 2))
    df_with_scores['BETTY SCORE'] = df_with_scores['BETTY SCORE'].apply(lambda x: round(x, 2))
    df_with_scores.loc[(df['BETTY_ROOF_SCORE'] == 0), 'BETTY SCORE'] = 0
    df_with_scores.loc[(df['BETTY_DEMOGRAPHIC_SCORE'] == 0), 'BETTY SCORE'] = 0
    return df_with_scores