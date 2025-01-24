import sys
import numpy as np
import xlrd
import csv
import pandas as pd
import logging
import pathlib
import os
import ast
import json
import re
from states_dict import us_state_to_abbrev
from uszipcode import SearchEngine
from utils import processAddress
from helper.awsServices import download_file_from_s3,upload_s3
import requests
import numpy as np
search = SearchEngine()
import tempfile

REQUIRED_COLUMNS_GENERIC = [
    "owner_full_name",
    "mail_street_address",
    "mail_city",
    "mail_state_name_short_code",
    "mail_zip_code"
]

REQUIRED_COLUMNS_LAND = [
    "lot_area_sqft", "apn", "property_zip_code"
]

REQUIRED_COLUMNS_SINGLE =[
    "property_zip_code"
]

REQUIRED_COLUMNS_COMMERCIAL =[
    "property_zip_code"
]

REQUIRED_COLUMNS_MULTI =[
    "property_zip_code"
]    

REQUIRED_COLUMNS_ROOFING = [
    "property_address",
    "property_city",
    "property_state_name_short_code",
    "property_zip_code"
]

renamed_cols = {}

def process_address(row):
    row['mail_street_address'], row['mail_zip_code'], row['mail_city'], row['mail_state_name_short_code'] = processAddress(row['mail_full_address'])
    return row

def strip_non_numeric_and_convert_to_float(df, column_name):
    df[column_name] = df[column_name].fillna(0)
    df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9.]', '', str(x))).astype(float)
    return df

def find_zip_code_from_county(dataframe, row):
    try:
        county_name = row["property_county"].split(", ")[0].title()
        state_name = row["property_state_name_short_code"].upper()
        # returning zip code from first row of filtered data frame by keywords

        return dataframe.loc[
            dataframe['COUNTYNAME'].str.contains(county_name) &
            dataframe['STATE'].str.contains(state_name)
        ].iloc[0]["ZIP"].item()
    except:
        return None
    
def extract_mail_city_state(data):
    mail_city = None
    mail_state = None
    if (pd.isna(data)):
        return None, None
    mail_city_state_parts = data.split()
    if (" ".join(mail_city_state_parts[:-1])):
        mail_city = " ".join(mail_city_state_parts[:-1])
    if (mail_city_state_parts[-1]):
        mail_state = mail_city_state_parts[-1]
    return pd.Series([mail_city, mail_state])

def extract_zip_code_from_uszips(lat, long):
    if pd.isna(lat) or pd.isna(long):
        return None
    x = [lat, long]
    if(len(str(x[0]))>0 and len(str(x[1]))>0):
        z = search.by_coordinates(float(x[0]), float(x[1]), radius=30)
        return z[0].zipcode
    else:
        return None 
    
def extract_zip_code(address):
    if (pd.isna(address)):
        return None
    address_parts = address.split()
    zip_code = None
    if address_parts:
        zip_code_groups = re.search(r'\b\d{5}(?:[-\s]\d{4})?\b', address_parts[-1])
        if zip_code_groups:
            zip_code = zip_code_groups.group(0)
    return zip_code

def abbrevate_state_names(state):
  try: 
    state = str(state)
    if(len(state) == 2):
        return state
    else:
      if state.title() in us_state_to_abbrev:
        return us_state_to_abbrev[state.title()] #get the abbrevation and assign it to the state names
      else:
        return None
  except Exception as e:
    logging.error("State not found ", e)
    return None

def is_full_address(address):
    if (pd.isna(address)):
        return None
    # Define regular expressions to match a city, state, and zip code
    #Commenting this out because for some reason doesn't catch city in "711 CEDAR SPRINGS DR, JACKSONVILLE, AL 36265-6031", possibly due to the ,
    #city_regex = r'\b[A-Z][a-z]+\b'
    state_regex = r'\b[A-Z]{2}\b'
    zip_regex = r'\b\d{5}(?:-\d{4})?\b'
    
    # Use regular expressions to check if the address contains a city, state, and zip code
    contains_state = bool(re.search(state_regex, address))
    contains_zip = bool(re.search(zip_regex, address))
    # If all three are present, it's a full address; otherwise, it's just a street address
    if contains_state and contains_zip:
        return True
    else:
        return False      

def has_valid_data(df, column):
    # Check if column exists
    if column not in df.columns:
        return False
    
    # Create a mask for valid values:
    # - Not NaN
    # - Not empty string after converting to string and stripping
    # - Not 'nan' or 'NaN' strings
    valid_mask = (
        ~df[column].isna() &  # Not NaN
        df[column].astype(str).str.strip().str.len() > 0 &  # Not empty after stripping
        ~df[column].astype(str).str.lower().isin(['nan'])  # Not 'nan' string
    )
    
    # Return True if there's at least one valid value
    return valid_mask.any()

def create_columns_if_doesnt_exist(df):
    if 'owner_full_name' not in df.columns:
        if has_valid_data(df, 'owner_mailing_name'):
            df['owner_full_name'] = df['owner_mailing_name'].astype(str).replace('nan ', '', regex=False)
        elif (has_valid_data(df, 'owner_first_name') or has_valid_data(df, 'owner_last_name')):
            first_name = df['owner_first_name'].fillna('')
            last_name = df['owner_last_name'].fillna('')
            
            df['owner_full_name'] = (first_name + ' ' + last_name).str.strip()
            
            df.loc[df['owner_full_name'] == '', 'owner_full_name'] = 'UNKNOWN'
        elif has_valid_data(df, 'mail_name'):
            df['owner_full_name'] = df['mail_name'].astype(str).replace('nan ', '', regex=False)
        elif has_valid_data(df, 'owner_first_name'):
            df['owner_full_name'] = df['owner_first_name']
        else:
            df['owner_full_name'] = 'UNKNOWN'
    
    # Clean any remaining 'nan' prefixes and ensure no empty values
    df['owner_full_name'] = df['owner_full_name'].astype(str).replace('nan ', '', regex=False)
    df['owner_full_name'] = df['owner_full_name'].replace(r'^\s*$', 'UNKNOWN', regex=True).fillna('UNKNOWN')

    if ('lot_area_sqft' not in df.columns):
        if ('lot_acreage' in df.columns):
            df = strip_non_numeric_and_convert_to_float(df, 'lot_acreage')
            df['lot_area_sqft'] = df['lot_acreage'].apply(pd.to_numeric) * 43560
            
        elif ('lot_acreage' not in df.columns) and ('lot_area' in df.columns):
            df = strip_non_numeric_and_convert_to_float(df, 'lot_area')
            if df['lot_area'].mean() < 100:
                df['lot_area_sqft'] = df['lot_area'].apply(pd.to_numeric) * 43560
            else:
                df['lot_area_sqft'] = df['lot_area']
    else:
        if ('lot_acreage' in df.columns):
            df = strip_non_numeric_and_convert_to_float(df, 'lot_acreage')
            df.loc[df['lot_area_sqft'].isna(), 'lot_area_sqft'] = df.loc[df['lot_area_sqft'].isna(), 'lot_acreage'].apply(pd.to_numeric) * 43560
        elif ('lot_acreage' not in df.columns) and ('lot_area' in df.columns):
            df = strip_non_numeric_and_convert_to_float(df, 'lot_area')
            if df['lot_area'].mean() < 100:
                df.loc[df['lot_area_sqft'].isna(), 'lot_area_sqft'] = df.loc[df['lot_area_sqft'].isna(), 'lot_area'].apply(pd.to_numeric) * 43560
            else:
                df.loc[df['lot_area_sqft'].isna(), 'lot_area_sqft'] = df.loc[df['lot_area_sqft'].isna(), 'lot_area']
            
            
    if 'property_state_name_short_code' in df.columns:
      df['property_state_name_short_code'] =  df.apply(lambda row: abbrevate_state_names(row['property_state_name_short_code']), axis=1)
      
    if 'mail_state_name_short_code' in df.columns:
        df['mail_state_name_short_code'] = df.apply(lambda row: abbrevate_state_names(row['mail_state_name_short_code']), axis=1)
        
    if 'property_zip_code' not in df.columns:
        df['property_zip_code'] = None
    
    #if('property_latitude' in df.columns) and ('property_longitude' in df.columns):
    #    df.loc[df['property_zip_code'].isna(), 'property_zip_code'] = df.loc[df['property_zip_code'].isna()].apply(lambda row: extract_zip_code_from_uszips(row['property_latitude'], row['property_longitude']), axis=1)
    
    if 'property_address_full' in df.columns:
        df.loc[df['property_zip_code'].isna(), 'property_zip_code'] = df.loc[df['property_zip_code'].isna(), 'property_address_full'].apply(lambda row: extract_zip_code(row))
        
    if 'property_county' in df.columns and 'property_state_name_short_code' in df.columns:
        zip_county = pd.read_csv(os.path.join(pathlib.Path(__file__).parent.resolve(), 'ZIP-COUNTY-FIPS_2017-06.csv'), low_memory = False)
        # iterate through each row of dataframe and populate the newly added column with zip codes after lookup
        df.loc[df['property_zip_code'].isna(), 'property_zip_code'] = df.loc[df['property_zip_code'].isna()].apply(lambda row: find_zip_code_from_county(zip_county, row), axis = 1) 
        
            
    if ('mail_full_address') in df.columns:
        df['is_full_address'] = df.apply(lambda row: is_full_address(row['mail_full_address']), axis=1)
        try:
            if ((df['is_full_address']==False).sum()/df.shape[0] > 0.5):
                df.rename(columns={'mail_full_address': 'mail_street_address'}, inplace=True)
            else:
                df = df.apply(lambda row: process_address(row), axis=1)
        except:
            pass
        
        
    if 'mail_zip_code' not in df.columns:
        if 'owner_city_state_zip' in df.columns:
            df['mail_zip_code'] = df.apply(lambda row: extract_zip_code(row['owner_city_state_zip']), axis=1)
    else:
        if 'owner_city_state_zip' in df.columns:
            df.loc[df['mail_zip_code'].isna(), 'mail_zip_code'] = df.loc[df['mail_zip_code'].isna(), 'owner_city_state_zip'].apply(lambda row: extract_zip_code(row))
            
    if 'mail_city_state' in df.columns and 'mail_state_name_short_code' not in df.columns:
        df[['mail_city', 'mail_state_name_short_code' ]] = df.apply(lambda row: extract_mail_city_state(row['mail_city_state']), axis=1)
    return df

def csv_from_excel(excel_path, csv_path):
    wb = xlrd.open_workbook(excel_path)
    sh = wb.sheet_by_index(0)
    your_csv_file = open(csv_path, 'w')
    wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)

    for rownum in range(sh.nrows):
        wr.writerow(sh.row_values(rownum))

    your_csv_file.close()

def rename_columns(file_path, column_mapping):
    df = pd.read_csv(file_path, low_memory = False, on_bad_lines='warn')
    df.rename(columns=column_mapping, inplace=True)
    return df

def required_columns_exist(df, industry_profile):
    missing_columns = []
    true_required_columns = REQUIRED_COLUMNS_GENERIC.copy()
    
    if industry_profile:
        if industry_profile.get('industryType') == 'roofing':
            # For roofing, check if we have property address fields instead of mailing address fields
            property_to_mail_mapping = {
                'property_address': 'mail_street_address',
                'property_city': 'mail_city',
                'property_state_name_short_code': 'mail_state_name_short_code',
                'property_zip_code': 'mail_zip_code'
            }
            
            # Remove mailing address fields from required columns if corresponding property fields exist
            for prop_field, mail_field in property_to_mail_mapping.items():
                if prop_field in df.columns and mail_field in true_required_columns:
                    true_required_columns.remove(mail_field)
            
            # Add roofing-specific required columns
            true_required_columns.extend(REQUIRED_COLUMNS_ROOFING)
            
        elif industry_profile.get('industryType') == 'real_estate_investors':
            if 'land' in industry_profile.get('property_type', []):
                true_required_columns.extend(REQUIRED_COLUMNS_LAND)
            if 'single' in industry_profile.get('property_type', []):
                true_required_columns.extend(REQUIRED_COLUMNS_SINGLE)
            if 'commercial' in industry_profile.get('property_type', []):
                true_required_columns.extend(REQUIRED_COLUMNS_COMMERCIAL)
            if 'multi' in industry_profile.get('property_type', []):
                true_required_columns.extend(REQUIRED_COLUMNS_MULTI)
    
    # Remove duplicates from required columns list
    true_required_columns = list(dict.fromkeys(true_required_columns))
    
    for column_name in true_required_columns:
        if column_name not in df.columns:
            missing_columns.append(column_name)
            
    if missing_columns:
        return False, missing_columns
    else:
        return True, []

     
def map_columns_new(file_name,new_name_dict, industry_profile , customer_email):
    logging.info('in map_columns')
    renamed_cols={}
    # industry_profile_json = json.loads(industry_profile)
    new_file_name = file_name.split('.csv')[0]+'_rwr.csv'
    #downloading file from S3
    res= download_file_from_s3(file_name ,customer_email,3600 )
    logging.info(f"Pre-signed URL generated: {res}")
    #creating a temporary file
    target_directory = os.path.join(os.path.dirname(__file__), '../../.sandbox')
    
    # Ensure the directory exists
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
        logging.info(f"Created target directory: {target_directory}")
    
    # Set the final file path
    file_path = os.path.join(target_directory, f"{file_name}")
    new_file_path = os.path.join(target_directory, f"{new_file_name}")
    
    # Download the file using the pre-signed URL
    response = requests.get(res)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logging.info("File downloaded successfully.")
    else:
        logging.error(f"Failed to download the file. Status code: {response.status_code}")
        raise Exception(f"Failed to download the file. HTTP status code: {response.status_code}")
    
 
    
    df = pd.read_csv(file_path, low_memory = False, on_bad_lines='warn')
    logging.info(df)
    if file_path.lower().endswith('.xlsx'):
    

        csv_path = file_path.lower().split('.xlsx')[0]+'.csv'
        csv_from_excel(file_path, csv_path)
        file_path = csv_path
    # If caller of the script sends the column mapping, just use that
    source = ""
    df_renamed = pd.DataFrame()
     

    if new_name_dict:
        logging.info("column_mapping - using custom mapping", new_name_dict)
        df_renamed = rename_columns(file_path, new_name_dict)
        source = "custom"
    # If caller has not sent column mapping then check if it's PRYCD or DataTree and process accordingly
    else:
        # Determine which mapping file to use based on industry type
        mapping_file = 'column_mappings.json'
        if industry_profile and industry_profile.get('industryType') == 'roofing':
            mapping_file = 'column_mapping_roofing.json'
            logging.info(f"Using roofing-specific mapping file: {mapping_file}")
        with open(os.path.join(pathlib.Path(__file__).parent.resolve(), mapping_file)) as f_in:
            df_mappings = json.load(f_in)
        
        # This is where we're renaming the columns from the original file
        for column_name in df.columns:
            if column_name.lower().strip() in df_mappings:
                renamed_cols[column_name] = df_mappings[column_name.lower().strip()]
                df.rename(columns = {column_name:df_mappings[column_name.lower().strip()]}, inplace = True)
        df_renamed = df


    # There's a chance the above process might create duplicate columns, the below will remove dupliate columns:
    # https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns
    df_renamed = df_renamed.loc[:,~df_renamed.columns.duplicated()].copy()
    # Doing some data massaging here 
    df_renamed_appended = create_columns_if_doesnt_exist(df_renamed)
    
    all_columns_present, missing_columns = required_columns_exist(df_renamed_appended, industry_profile)
    if not all_columns_present:
        # if source == "custom":
        #     error_message = "column_mapping.py -- Required columns don't exist for " + source + " file, columns missing are: " + ', '.join(missing_columns) + ", column mapping used: " + str(new_name_dict)
        # else:
        error_message = "column_mapping.py -- Required columns don't exist for " + source + " file, columns missing are: " + ', '.join(missing_columns)
        raise Exception({"error_message":error_message,"mapped_cols":renamed_cols})
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.info(f"Temporary file deleted: {file_path}")
        except Exception as e:
            logging.error(f"Failed to delete temporary file: {file_path}. Error: {e}")
    
    df_renamed_appended.to_csv(new_file_path, index=False)
    upload_response= upload_s3(new_file_name, new_file_path, customer_email)

 
    return new_file_name, renamed_cols
    