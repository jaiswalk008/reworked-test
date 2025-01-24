import logging

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()]
)

# Create a logger for this module
logger = logging.getLogger(__name__)

from unittest import removeResult
import pandas as pd
import os
import pymongo
import json
from dotenv import load_dotenv
import numpy as np
import sys
import pgeocode as pgeo
import requests
import re
from datetime import datetime
from states_dict import us_state_to_abbrev
import gender_guesser.detector as gender
from utils import (
    processAddress,
    clean_and_split_owner_names,
    process_names_with_llm,
    standardize_name_order_LLM
)
from geocode import geocodeFromDataFrame
from buildingInsights import buildingInsightsFromDataFrame
from datetime import datetime
import pytz
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

load_dotenv()
dbclient = pymongo.MongoClient(os.getenv('MONGO_URL'))
mydb= dbclient['myFirstDatabase']
demographicDataCollection = mydb["DemographicData"]
propertyDataCollection = mydb["PropertyData"]
businessDataCollection = mydb["BusinessData"]
ageDataCollection = mydb["AgeData"]

# API configuration for Property Lookup
PROPERTY_API_KEY = "cKEf_4HZGh_L5EV98iXjXC**"
PROPERTY_BASE_URL = "https://property.melissadata.net/v4/WEB/LookupProperty"

ED_API_KEY = "eb67e7cd22a94b67436e3719"
ED_BASE_URL = "https://api.data-axle.com/v2/people/match"
ED_HEADERS = {"X-AUTH-TOKEN": ED_API_KEY}

# Global variables for Property Lookup
REQUESTS_PER_SECOND = 10
MAX_WORKERS = 10
required_property_api_fields = [
    'PropertyUseInfo_PropertyUseGroup',
    'BuildingInfo_YearBuilt',
    'BuildingInfo_RoofType',
    'BuildingInfo_RoofCover',
    'SalesInfo_LastSalePrice',
    'CurrentDeed_MortgageDueDate', 'CurrentDeed_MortgageDate',
    'SaleInfo_AssessorLastSaleDate', 'SaleInfo_LastOwnershipTransferDate',
    'PrimaryOwner_Name2First', 'PrimaryOwner_Name2Last',
    'CurrentDeed_MortgageAmount', 'SaleInfo_DeedLastSalePrice',
    'PropertyUseInfo_YearBuilt', 'CurrentDeed_SecondMortgageAmount',
    'PropertyUseInfo_PropertyUseStandardized', 'OwnerAddress_OwnerOccupied',
    'Parcel_CBSACode', 'OwnerAddress_State', 'Tax_AssessedValueTotal',
    'PropertyUseInfo_PropertyUseGroup',
    'PropertyUseInfo_PropertyUseType',   # More detailed property use info
    'BuildingInfo_YearBuilt',            # Age of the building
    'BuildingInfo_RoofType',             # Type of roof
    'BuildingInfo_RoofCover',            # Roof covering material
    'BuildingInfo_TotalSquareFeet',      # Building size
    'BuildingInfo_Stories',              # Number of stories
    'LotInfo_LotSquareFeet',            # Lot size
    'SalesInfo_LastSaleDate',            # Last sale date
    'SalesInfo_LastSalePrice',           # Last sale price
    'SalesInfo_LastSaleType',            # Type of sale
]

"""" Converts boolean equivalents to boolean, takes as input a column as well as what to do if null """
def transform_to_boolean(column, replace_null_with):
    mapping = {
        "TRUE": True,
        "True": True,
        "true": True,
        "1": True,
        1: True,
        "YES": True,
        "Yes": True,
        "yes": True,
        "FALSE": False,
        "False": False,
        "false": False,
        "0": False,
        0: False,
        "NO": False,
        "No": False,
        "no": False,
        pd.isna: replace_null_with,  # Handle null values
        None: replace_null_with,   # Handle None values,
        '': replace_null_with
    }
    return column.apply(lambda x: mapping.get(x, x))

"""Checks if an address is an apartment"""
def is_apartment(address, property_use_group=None):
    # Check property use group first if available
    if property_use_group and not pd.isna(property_use_group):
        multi_unit_indicators = [
            "Multi-Unit/Multi-Family Residential"
        ]
        return any(indicator.lower() in str(property_use_group).lower() for indicator in multi_unit_indicators)
    
    # Fallback to address-based detection
    keywords = ["apt", "apartment", "#", "unit"]
    return any(keyword.lower() in str(address).lower() for keyword in keywords)

"""Checks if mobine number is present in the row, returns boolean"""
def mobile_number_present(row):
  if ('phone1' in df.columns) and (pd.notnull(row['phone1'])):
    if 'phone1_type' in df.columns:
      if row["phone1_type"] == "Mobile":
        return True
      if ('phone2' in df.columns) and (pd.notnull(row['phone2'])):
        if 'phone2_type' in df.columns:
          if row['phone2_type'] == 'Mobile':
            return True
      if ('phone3' in df.columns) and (pd.notnull(row['phone3'])):
        if 'phone3_type' in df.columns:
          if row['phone3_type'] == 'Mobile':
            return True
    else:
      return True
  else:
    if ('phone2' in df.columns) and (pd.notnull(row['phone2'])):
      return True
    elif ('phone3' in df.columns) and (pd.notnull(row['phone3'])):
      return True
  return False

"""Calling PostGrid a second time with full address this time if BOTH Melissa failed AND Postgrid failed with address parts"""
def verify_address_second(row):
    try:
        url = "https://api.postgrid.com/v1/addver/verifications"
        headers = {'x-api-key': 'live_sk_orcFDruWxRV7TXgzdZeBwr', 'Content-Type': 'application/json'}
        response = ''
        requiredKeys = {'line1', 'postalOrZip'}
        address = {'country': 'us'}
        if 'mail_full_address' in row:
            address['line1'], address['city'], address['state'], address['postalOrZip'] = processAddress(row['mail_full_address'])
        else:
            address['line1'], _, _, _ = processAddress(row['mail_street_address']) if 'mail_street_address' in row and not pd.isnull(row['mail_street_address']) else (None, None, None, None)
            address['city'] = row['mail_city'] if 'mail_city' in row and not pd.isnull(row['mail_city']) else None
            address['state'] = row['mail_state_name_short_code'] if 'mail_state_name_short_code' in row and not pd.isnull(row['mail_state_name_short_code']) else None
            address['postalOrZip'] = row['mail_zip_code'] if 'mail_zip_code' in row and not pd.isnull(row['mail_zip_code']) else None
        if requiredKeys.issubset(address.keys()) and all(((address[key] is not None) and (not address[key] == "")) for key in requiredKeys):
            response = requests.request("POST", url, headers=headers, json={'address': address}, timeout=5)
            response_data = response.json()['data']
            if response_data['status'] == 'failed':
                return row
            else:
              # Assuming you already have the address components in response_data
              row["BETTY_UPDATED_ADDRESS_LINE1"] = response_data.get('line1', '').strip()
              row["BETTY_UPDATED_ADDRESS_LINE2"] = response_data.get('line2', '').strip()
              row["BETTY_UPDATED_ADDRESS_CITY"] = response_data.get('city', '').strip()
              row["BETTY_UPDATED_ADDRESS_STATE"] = response_data.get('provinceOrState', '').strip()
              row["BETTY_UPDATED_ADDRESS_ZIP"] = response_data.get('postalOrZip', '').strip()
              
              row["demo_address_verification_failed"] = False
              return row
        else:
            return row
    except Exception as e:
       logging.error(" in verify_address_second exception", e)
       return row
        # return True

""" Calling Postgrid with address parts first if Melissa address verification failed """
def verify_address(row):
  # print("aa")
  url = "https://api.postgrid.com/v1/addver/verifications"
  try:
    address = row['mail_full_address']
  except:
    address="" 

  if address=="":

    if pd.isnull(row['mail_state_name_short_code']) or pd.isnull(row['mail_street_address']) or pd.isnull(row['mail_city']) or pd.isnull(row['mail_zip_code']):
      return row

    try:
      street_address = row['mail_street_address']
      city = row['mail_city']
      state = row['mail_state_name_short_code']
      zip_code = row['mail_zip_code']
      address=f'{street_address} {city} {state} {zip_code}'
    except:
      # This could be hit if that particular column doesn't exist
      return row  
      
  headers = { 'x-api-key': 'live_sk_orcFDruWxRV7TXgzdZeBwr'}
  response_data = ''
  try:
    response = requests.request("POST", url, headers=headers, data={'address': address}, timeout=5)
    response_data = json.loads(response.text)['data']
    print("json.loads(response.text)", response_data)
    if response_data['status'] == 'failed':
      row = verify_address_second(row)
      return row
    else:
      row["BETTY_UPDATED_ADDRESS_LINE1"] = response_data.get('line1', '').strip()
      row["BETTY_UPDATED_ADDRESS_LINE2"] = response_data.get('line2', '').strip()
      row["BETTY_UPDATED_ADDRESS_CITY"] = response_data.get('city', '').strip()
      row["BETTY_UPDATED_ADDRESS_STATE"] = response_data.get('provinceOrState', '').strip()
      row["BETTY_UPDATED_ADDRESS_ZIP"] = response_data.get('postalOrZip', '').strip()


      row["demo_address_verification_failed"] = False
      return row
  except Exception as e:
    logging.error("Exception in verify_address:", e)
    row = verify_address_second(row)
    return row

def get_melissa_business_data(df):
    fieldMapping = {'EmployeesEstimate': 'employees_estimate', 'SalesEstimate': 'sales_estimate'}
    for index, row in df.iterrows():
        if row['is_business'] or row['is_public_entity'] and not pd.isnull(row['owner_full_name']):
            zip_code = None
            address = None
            full_name = row['owner_full_name']
            try:
                if 'mail_full_address' in row:
                    address = row['mail_full_address']
                    _,  _, _, zip_code = processAddress(address)
                elif pd.isnull(row['mail_state_name_short_code']) or pd.isnull(row['mail_street_address']) or pd.isnull(row['mail_city']) or pd.isnull(row['mail_zip_code']):
                    continue  # Skip to the next row
                else:
                    street_address = row['mail_street_address']
                    city = row['mail_city']
                    state = row['mail_state_name_short_code']
                    zip_code = row['mail_zip_code']
                    address = street_address, city, state, zip_code
            except:
                continue  # Skip to the next row
            company_name_pattern = re.compile(re.escape(full_name), re.IGNORECASE)
            query = {"CompanyName": {"$regex": company_name_pattern}}
            if businessDataCollection.count_documents(query) > 0:
                response = businessDataCollection.find(query)
                response = response[0]
            else:
                try:
                    if not (address is None or address == "") and not (zip_code is None or zip_code == "") and not (full_name is None or full_name == ""):
                        # print("**************calling api*****************")
                        cols = "AddressLine1,CensusBlock,CensusTract,City,CompanyName,CountryCode,CountryName,CountyFIPS,CountyName,DeliveryIndicator,EIN,EmployeesEstimate,Latitude,LocationType,Longitude,MelissaEnterpriseKey,MelissaAddressKey,MelissaAddressKeyBase,NAICSCode1,NAICSCode2,NAICSCode3,NAICSDescription1,NAICSDescription2,NAICSDescription3,Phone,PlaceCode,PlaceName,Plus4,PostalCode,RecordID,Results,SICCode1,SICCode2,SICCode3,SICDescription1,SICDescription2,SICDescription3,SalesEstimate,State,StockTicker,Suite,TotalContacts,WebAddress,Contacts"
                        url = f"https://businesscoder.melissadata.net/WEB/BusinessCoder/doBusinessCoderUS?id=cKEf_4HZGh_L5EV98iXjXC**&cols={cols}&opt=&rec=&comp={full_name}&a1={address}&a2=&city=&state=&postal={zip_code}&ctry=us"
                        response = requests.get(url, timeout=5)
                        response = response.json()['Records'][0]
                        # print(json.dumps(response, indent=2))
                        #creating a new field uploaded_at
                        response['uploaded_at'] = datetime.now(pytz.utc)

                        x = businessDataCollection.insert_one(response)
                    else:
                        continue
                except Exception as e:
                    logger.error("hit error fetching data from Melissa", e)
                    continue  # Skip to the next row

            for field in fieldMapping:
                if field in response:
                    column_name = fieldMapping[field]
                    df.at[index, column_name] = response[field]

    return df

""" Function that returns first name from full name"""
def get_first_name(full_name):
    if "," in full_name:
      name_parts = full_name.split(',')
      return name_parts[1].split()[0]
    elif full_name != '':
      return full_name.split()[0]
    else:
      return None

def get_age_ed(row, df):
    first_name = None
    last_name = row.get('owner_last_name', None)
    address = row.get('BETTY_UPDATED_ADDRESS_LINE1', None)
    city = row.get('BETTY_UPDATED_ADDRESS_CITY', None)
    state = row.get('BETTY_UPDATED_ADDRESS_STATE', None)
    zip_code = row.get('BETTY_UPDATED_ADDRESS_ZIP', None)
    
    try:
        if 'owner_first_name' in df.columns and pd.notnull(row['owner_first_name']):
            first_name = row['owner_first_name'].split()[0]
        elif 'owner_mailing_name' in df.columns and pd.notnull(row['owner_mailing_name']):
            first_name = get_first_name(row['owner_mailing_name'])
        elif 'owner_full_name' in df.columns and pd.notnull(row['owner_full_name']):
            first_name = get_first_name(row['owner_full_name'])

        if first_name is None or first_name == '':
            logger.warning(f"No valid first name found for row: {row}")
            return None, None

        normalized_zip_code = zip_code.split('-')[0] if zip_code else None
        normalized_address = address.strip().lower() if address else None

        ed_data = ageDataCollection.find_one({
            "first_name": first_name,
            "last_name": last_name,
            "address": normalized_address,
            "zip_code": normalized_zip_code,
            "source": "ED"
        })

        if ed_data is not None and 'age' in ed_data:
            logger.info(f"ED age data found in database for {first_name} {last_name}: {ed_data['age']}")
            return ed_data['age'], "ED"
        else:
            logger.warning(f"No ED data found in the database for {first_name} {last_name}")

        if not row.get('age_source') and last_name and address and city and state and zip_code:
            params = {
                "first_name": first_name,
                "last_name": last_name,
                "street": address,
                "city": city,
                "state": state,
                "postal_code": normalized_zip_code,
            }
            data = {
                "identifiers": params,
                "packages": ["enhanced_v2", "generations_v3"]
            }

            try:
                response = requests.post(ED_BASE_URL, headers=ED_HEADERS, json=data, timeout=10)
                if response.status_code == 200:
                    api_data = response.json()
                    logger.info(f"ED API response for {first_name} {last_name}: {api_data}")
                    
                    document = api_data.get('document', {})
                    attributes = document.get('attributes', {})
                    age = attributes.get('age')

                    if age is not None:
                        ageDataCollection.update_one(
                            {
                                "first_name": first_name,
                                "last_name": last_name,
                                "address": normalized_address,
                                "zip_code": normalized_zip_code,
                                "source": "ED"
                            },
                            {"$set": {"age": age, "timestamp": datetime.now(pytz.utc)}},
                            upsert=True
                        )
                        logger.info(f"ED API returned and stored age: {age} for {first_name} {last_name}")
                        return age, "ED"
                    else:
                        logger.warning(f"ED API did not return age data for {first_name} {last_name}")
                        return None, None

                elif response.status_code == 429:
                    logger.warning("ED API rate limit exceeded. Retrying after delay.")
                    #TODO: Undo this once you have a contract with ED
                    return None, None
                    #time.sleep(60)
                    #return get_age_ed(row, df)  # Retry the API call

                else:
                    logger.error(f"Unexpected ED API status code: {response.status_code}")
                    return None, None

            except requests.exceptions.Timeout:
                logger.error(f"ED API request timed out for {first_name} {last_name}")
                return None, None

            except requests.exceptions.RequestException as e:
                logger.error(f"ED API request failed for {first_name} {last_name}: {e}")
                return None, None

    except Exception as e:
        logger.error(f"Error in get_age_ed for {first_name}: {e}")

    return None, None

def get_age_agify(row, df):
    first_name = None
    try:
        if 'owner_first_name' in df.columns and pd.notnull(row['owner_first_name']):
            first_name = row['owner_first_name'].split()[0]
        elif 'owner_mailing_name' in df.columns and pd.notnull(row['owner_mailing_name']):
            first_name = get_first_name(row['owner_mailing_name'])
        elif 'owner_full_name' in df.columns and pd.notnull(row['owner_full_name']):
            first_name = get_first_name(row['owner_full_name'])

        if first_name is None or first_name == '':
            logger.warning(f"No valid first name found for row: {row}")
            return None, None

        agify_data = ageDataCollection.find_one({
            "first_name": first_name,
            "source": "Agify"
        })

        if agify_data and 'age' in agify_data:
            logger.info(f"Agify age data found in database for {first_name}: {agify_data['age']}")
            return agify_data['age'], "Agify"

        response = requests.get(f"https://api.agify.io?name={first_name}&country_id=US&apikey=01eb88b54c29db19764c2d177d7b25c1", timeout=3)
        if response.status_code == 200:
            age = response.json().get('age')
            if age is not None:
                ageDataCollection.update_one(
                    {"first_name": first_name, "source": "Agify"},
                    {"$set": {"age": age, "timestamp": datetime.now(pytz.utc)}},
                    upsert=True
                )
                logger.info(f"Agify API returned age: {age} for {first_name}. Stored in database.")
                return age, "Agify"
    except Exception as e:
        logger.error(f"Error in get_age_agify for {first_name}: {e}")

    return None, None

"""Function to get age given the first name
It takes datframe as an argument and returns age"""
def get_ED_age(row, df):
    if 'potential_age' in row and pd.notnull(row['potential_age']):
        logger.info(f"Age data already available: {row['potential_age']} (Source: {row['age_source']})")
        return row['potential_age'], row['age_source']

    # First, attempt to get age from ED
    age, source = get_age_ed(row, df)
    if age is not None:
        logger.info(f"Age retrieved from ED: {age} for row {row['owner_full_name']}")
        return age, source

    # # If ED does not provide age, attempt to get age from Agify
    # age, source = get_age_agify(row, df)
    # if age is not None:
    #     logger.info(f"Age retrieved from Agify: {age} for row {row['owner_full_name']}")
    #     return age, source

    # If all sources fail, return None
    logger.warning(f"No age data found for row: {row['owner_full_name']}")
    return None, None

def get_approximate_Agify_age(row, df):
    if 'potential_age' in row and pd.notnull(row['potential_age']) and str(row['potential_age']).strip() != "":
        logger.info(f"Age data already available: {row['potential_age']} (Source: {row['age_source']})")
        return row['potential_age'], row['age_source']

    # # First, attempt to get age from ED
    # age, source = get_age_ed(row, df)
    # if age is not None:
    #     logger.info(f"Age retrieved from ED: {age} for row {row['owner_full_name']}")
    #     return age, source

    # If ED does not provide age, attempt to get age from Agify
    age, source = get_age_agify(row, df)
    if age is not None:
        logger.info(f"Age retrieved from Agify: {age} for row {row['owner_full_name']}")
        return age, source

    # If all sources fail, return None
    logger.warning(f"No age data found for row: {row['owner_full_name']}")
    return None, None
    
""" Function to check if given name is a buiseness or not. 
It takes dataframe aa an argument and returns a boolean value """
def get_is_business(row):
    business_names = ['LLC', 'LTD', 'INC', 'FAMILY', 'CHURCH', 'TRUST', 'TRUSTEE', 'TRUSTEES', 'ASSET', 'INC', 'VENTURES', 'ESTATE', 'EST', 'MORTGAGE', 'EQUITY', 'HOLDINGS']
    names = ''
    if 'owner_full_name' in row: 
      names = (str(row['owner_full_name']).upper()).split(' ')
    return True if (np.in1d(business_names, names)).any() else False

""" Function to check if given name is a public entity or not. 
It takes dataframe aa an argument and returns a boolean value """
def get_is_public_entity(row):
  names=''
  govt_entity_names = ['GOVERNMENT', 'IRRIGATION', 'WATER', 'MUNICIPALITY', 'CITY', 'STATE','STATES', 'AMERICA', 'USA', 'SCHOOL',
                       'UNIVERSITY', 'COUNTY', 'DISTRICT', 'POWER', 'LIGHT', 'BANK', 'TITLE', 'ESCROW', 'ASSOCIATION', 'HOME', 'GAS', 'CORPORATION',
                        'CORP', 'GOVT', 'TOWN', 'INSTITUTE', 'WAL-MART', 'WALMART', 'DEPARTMENT', 'DEPT']
  if 'owner_full_name' in row:
    names = (str(row['owner_full_name']).upper()).split(' ')
  return True if (np.in1d(govt_entity_names, names)).any() else False

""" Function to convert zipcodes to proper format 
It takes a string zipcode as an argument and returns converted int zipcode"""
def handle_zipcode(mail_zip):
  try:
    mail_zip= str(mail_zip).replace('=','').replace('"', '').replace('.0','').replace(',','').replace('$','')
    if not (str(mail_zip)[:5].isdigit()):
      return None
    if(len(str(mail_zip)) ==  5):
      return str(mail_zip)
    elif(len(str(mail_zip)) > 5):
      mail = str(mail_zip)[:5]
      return mail
    elif(len(str(mail_zip)) < 5):
      mail = "0"*(5-len(str(mail_zip))) + str(mail_zip) #add 0 at end of zipcodes which have length less than 5
      return mail    
  except Exception as e:
    logger.error("Error in mail_zip",e)
    return None

""" Function to handle the do_not_mail columns 
It finds the maximum occuring element keeps it as it is abd change others to 1"""
def handle_do_not_mail(freq_mode, row):
    try:
      if freq_mode == 'Y':
        if(row["do_not_mail"] == 'Y'):
          return True
      if(row["do_not_mail"] != freq_mode):
          return True  #if the element is not mode set it as 1
      else:
          return False #keep the mode as it is
    except Exception as e:
      logger.error("Error in do not mail ", e)
      return None

""" Function to handle the owner_type columns 
It finds the maximum occuring element keeps it as it is and 
change others to Other or 2 depending on the datatype"""
def handle_owner_type(freq_mode,row):
  # For some weird reason mode changes case of string so need to change again so we can compare check
  if type(freq_mode) == str:
    freq_mode = freq_mode.lower()
    if row["owner_1_type"]:
      row["owner_1_type"] = str(row["owner_1_type"]).lower() 
  try:
    if(row["owner_1_type"] != freq_mode):
        return 2 #if the element is not mode set it as new value
    else:
        return 1 #keep the mode as it is
  except Exception as e:
    logger.error("Error in owner_1_type ", e)
    return None
  
""" Function to get distance between mail_zip_code and property_zip_code"""
def get_dist_btw_site_and_mail_zip(row, dist):
  try:
    mail_zip = row['mail_zip_code']
    if(mail_zip):
      prop_zip = row['property_zip_code']
      logger.debug("Succesfully handled distance")
      if mail_zip and prop_zip:
        return dist.query_postal_code(mail_zip, prop_zip) #get the distance between mail zipcode and property zipcode
  except Exception as e:
    logger.error("one of the zip codes missing, in row ", e)
    return None

""" Converts a yyyymm data format to age as of today """
def yyyymm_to_age(yyyymm_string):
    # Get the current date
    current_date = datetime.now()

    try:
        # Extract the year and month from the yyyymm_string
        year = int(yyyymm_string[:4])
        month = int(yyyymm_string[4:])

        # Create a datetime object for the given yyyymm
        birth_date = datetime(year, month, 1)

        # Calculate the age
        age = current_date.year - birth_date.year - ((current_date.month, current_date.day) < (birth_date.month, birth_date.day))

        return age
    except ValueError:
        #print("Invalid input format. Please provide a valid yyyymm string (e.g., '202307' for July 2023).")
        return None

""" Abbrevates the State names to their respective abbrevations """
def abbrevate_state_names(df,row,columns):
  try: 
    if(row in columns):
      new_row = []
      for state in df[row]:
        if(len(state) == 2):
          return df[row]
        else:
          if state in us_state_to_abbrev:
            new_row.append(us_state_to_abbrev[state]) #get the abbrevation and assign it to the state names
          else:
            new_row.append("NA")
      return new_row
  except Exception as e:
    logger.error("State not found ", e)
    return None

"""Convert string date to datetime object"""
def convert_to_date(df,columns):
  try: 
    for row in columns:
      if("Date" in row or "date" in row):
        df[row] = pd.to_datetime(df[row], format="%m/%d/%y", errors='coerce')    
    return df
  except Exception as e:
    logger.error("Error occured in datetime conversion")
    return df

"""Change the type of column based on their contents"""
def change_type(df,column_names,columns):
 
  for column_name in column_names:
    if(column_name in columns):
      df[column_name] = df[column_name].fillna(value=0)
      df[column_name] = df[column_name].replace({'\$': '', ',': '', '#':''}, regex=True)
      if(column_name == 'market_price_per_acre' or column_name == "property_tax" or column_name == "market_price"or column_name == "delinquent_tax_value" or column_name == "offer_price" or column_name == "last_sale_price"):    
        df[column_name] = df[column_name].astype(float, errors='ignore')
      if(column_name == "tax_year" or column_name == "delinquent_tax_year" 
         or column_name == "age" or column_name == 'num_bedrooms' or column_name == 'num_bathrooms' 
         or column_name == 'num_of_stories' or column_name == 'year_built'):
        df[column_name].replace(regex=True, inplace=True, to_replace=r'[^0-9]', value=r'0')
        df[column_name] = df[column_name].astype(int, errors = 'ignore')
      # df[column_name] = df[column_name].fillna(0)
      # df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9.]', '', str(x))).astype(int)
  return df


def get_gender_from_name(name):
    detector = gender.Detector()
    guessed_gender = detector.get_gender(name)
    # Map the guessed gender to 'M' or 'F'
    gender_mapping = {
        'male': 'M',
        'female': 'F',
        'mostly_female': 'F',
        'mostly_male': 'M'
    }
    # Return the mapped gender or the original guessed gender
    return gender_mapping.get(guessed_gender, guessed_gender)

"""Function that calls Melissa API and gets person data"""
def get_melissa_person_data(row):
    logging.info(f"Processing row: {row['owner_full_name']}")
    
    # Initialize BETTY columns
    betty_columns = ["BETTY_UPDATED_ADDRESS_LINE1", "BETTY_UPDATED_ADDRESS_LINE2", "BETTY_UPDATED_ADDRESS_CITY", "BETTY_UPDATED_ADDRESS_STATE", "BETTY_UPDATED_ADDRESS_ZIP"]
    for col in betty_columns:
        row[col] = ''

    row["BETTY_UPDATED_ADDRESS_LINE1"] = ''
    row["BETTY_UPDATED_ADDRESS_LINE2"] = ''
    row["BETTY_UPDATED_ADDRESS_CITY"] = ''
    row["BETTY_UPDATED_ADDRESS_STATE"] = ''
    row["BETTY_UPDATED_ADDRESS_ZIP"] = ''

    row['demo_address_verification_failed'] = False
    row['demo_currently_lives_in_address'] = True
    try:
        full_name = None
        # Try owner 1's name first
        o1_first = row.get('BETTY_UPDATED_O_1_F_N', '').strip()
        o1_last = row.get('BETTY_UPDATED_O_1_L_N', '').strip()
        
        # If owner 1's name is empty, try owner 2's name
        o2_first = row.get('BETTY_UPDATED_O_2_F_N', '').strip()
        o2_last = row.get('BETTY_UPDATED_O_2_L_N', '').strip()
        
        if o1_first and o1_last:
            full_name = o1_first + "%20" + o1_last
        elif o2_first and o2_last:
            full_name = o2_first + "%20" + o2_last
        elif o1_first:
            full_name = o1_first
        elif o1_last:
            full_name = o1_last
        elif o2_first:
            full_name = o2_first
        elif o2_last:
            full_name = o2_last
        else:
            full_name = None
        
        street_address = row.get('mail_street_address', None)
        city = row.get('mail_city', None)
        state = row.get('mail_state_name_short_code', None)
        zip_code = row.get('mail_zip_code', None)
    except:
        logging.error(f"Error accessing row data for {row.get('owner_full_name', 'Unknown')}")
        return row

    if any(pd.isnull(value) for value in [ street_address, zip_code]):
        logging.warning(f"Missing street address or zip code for {full_name}")
        return row

    street_address_cleaned = street_address.replace("#", "")

    street_address_encoded = str(street_address_cleaned).replace(" ", "%20")
    city_encoded = str(city).replace(" ", "%20") if city is not None else None

    response = None
    if demographicDataCollection.count_documents({"original_full_name": full_name, "title_case_street_address": street_address.title()}) > 0:
        logging.info(f"Melissa data found in db for {full_name}")
        response = demographicDataCollection.find_one({"original_full_name": full_name, "title_case_street_address": street_address.title()})
        response = response
    else:
        try:
            logging.info(f"Calling Melissa api for {full_name}")
            melissa_key = 'cKEf_4HZGh_L5EV98iXjXC**'
            # act = Move - to get updated address if person is moved to new address. Status code to identity if address is old or not is NS01
            melissa_url = f"https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify?t=&id={melissa_key}&act=Move"
            if industry_profile['industryType'] == 'real_estate_investors':
              cols = "AddressLine1,AddressLine2,AddressStreetName,CountryCode,DateOfBirth,Gender,DateOfDeath"
            elif industry_profile['industryType'] == 'insurance_provider':
                cols = "AddressLine1,AddressLine2,AddressStreetName,CountryCode,DateOfBirth,Gender,DateOfDeath,OwnRent"            
            elif industry_profile['industryType'] == 'solar_installer':
              cols = "AddressLine1,AddressLine2,AddressStreetName,CountryCode,Gender,ChildrenAgeRange,DateOfBirth,DateOfDeath,DemographicsGender,Education,HouseholdIncome,HouseholdSize,LengthOfResidence,MaritalStatus,PoliticalParty,PresenceOfChildren,OwnRent"
            else:
                cols = "AddressLine1,AddressLine2,AddressStreetName,CountryCode,DateOfBirth,Gender,DateOfDeath"
            melissa_url += f"&cols={cols}"

            # Dynamically generate query parameters
            params = {
                'full': full_name,
                'comp': '',
                'a1': street_address_encoded,
                'a2': '',
                'city': city_encoded,
                'state': state,
                'postal': zip_code,
                'email': '',
                'phone': '',
                'ctry': '',
                'ss': '',
                'mak': '',
                'ip': '',
                'bday': '',
                'bmo': '',
                'byr': '',
                'format': 'json'
            }

            complete_url = melissa_url + '&' + '&'.join(f"{k}={v}" for k, v in params.items() if v is not None)

            response = requests.get(complete_url, timeout=5)
            logging.info(f"Melissa API response status: {response.status_code}")
            if response.status_code == 200 and response.headers["content-type"].strip().startswith("application/json"):
                response = response.json()['Records'][0]
                logging.info(f"Melissa API response: {response}")
                response['original_full_name'] = full_name
                response['title_case_street_address'] = street_address.title()
                response['uploaded_at'] = datetime.now(pytz.utc)

               
                x = demographicDataCollection.insert_one(response)
            else:
                # Calling postgrid here due to failure of Melissa
                logging.warning(f"Melissa API call failed. Calling verify_address for {full_name}")
                return verify_address(row)
        except Exception as e:
          logging.error(f"Exception in Melissa API call for {full_name}: {str(e)}")
          row = verify_address(row)
          # logging.error("hit error fetching data from Melissa", e)
          return row
    # adding gender in file
    gender = response.get('Gender', '').strip()
    gender_source = "Melissa"
    # rowCount = row['Sr']
    # print("Record Count", rowCount)
    if hasattr(row, 'owner_first_name') and row.owner_first_name:
      # print("taking first name")
      first_name = row.owner_first_name
    else:
      if not (pd.isna(row.owner_full_name) or row.owner_full_name.strip() == ""):
        first_name = row.owner_full_name.split()[0]
      else:
         first_name = None
    # first_name = row.owner_full_name.split()[0]
    # print(f"Gender on name '{first_name}': {gender}")
    if gender not in ['F', 'M']:
        name = first_name
        gender = get_gender_from_name(name)
        gender_source = "gender_guessar"
        # print(f"Guessed gender based on name '{name}': {gender}")

    row["demo_gender"] = gender
    row["gender_source"] = gender_source
    row["mel_results"] = response.get('Results', '').strip()
    if industry_profile['industryType'] == 'solar_installer':
      row["demographicsresults"] = response.get('DemographicsResults', '').strip()
      row["education"] = response.get('Education', '').strip()
      row["householdincome"] = response.get('HouseholdIncome', '').strip()
      row["householdsize"] = response.get('HouseholdSize', '').strip()
      row["lengthofresidence"] = response.get('LengthOfResidence', '').strip()
      row["maritalstatus"] = response.get('MaritalStatus', '').strip()
      row["ownrent"] = response.get('OwnRent', '').strip()
      row["politicalparty"] = response.get('PoliticalParty', '').strip()
      row["presenceofchildren"] = response.get('PresenceOfChildren', '').strip()

    # Assuming you already have the address components
    row["BETTY_UPDATED_ADDRESS_LINE1"] = response.get('AddressLine1', '').strip()
    row["BETTY_UPDATED_ADDRESS_LINE2"] = response.get('AddressLine2', '').strip()
    row["BETTY_UPDATED_ADDRESS_CITY"] = response.get('City', '').strip()
    row["BETTY_UPDATED_ADDRESS_STATE"] = response.get('State', '').strip()
    row["BETTY_UPDATED_ADDRESS_ZIP"] = response.get('PostalCode', '').strip()
    # Construct the address without adding extra spaces for empty components
    
    logging.info(f"BETTY columns after assignment: {[row[col] for col in betty_columns]}")
    
    res = response['Results']
    # Ensure that the Melissa data doesn't overwrite the "ED" age source
    if len(response['DateOfBirth'].strip()) != 0:
        if 'age_source' not in row or not row['age_source'] or row['age_source'] == "Unknown":
            row['potential_age'] = yyyymm_to_age(response['DateOfBirth'])
            row['age_source'] = "Melissa"
    if len(response['DateOfDeath'].strip()) != 0:
      row['date_of_death'] = str(pd.to_datetime(response['DateOfDeath'][0:4], format='%Y', errors='coerce'))

    row["address_validation_source"] = "Melissa"
    # Refer https://wiki.melissadata.com/index.php?title=Result_Codes#AV_-_Address_Verification for codes
    #if re.search("AE..,", res) != None or re.search("AE..$", res) != None or "AS03" in res or "AS17" in res:
    # AE08-Sub Premise Number Invalid-An address element after the house number, in most cases the sub-premise, was not valid.
    # AE09-Sub Premise Number Missing-An address element after the house number, in most cases the sub-premise, was missing.
    # AS03-Non USPS Address Match-US Only. This US address is not serviced by the USPS but does exist and may receive mail through third party carriers like UPS.
    # AS12-Moved to New Address-The record moved to a new address.
    # VS01-Historical Address Match-The current address is outdated and a newer address match was found. Use the "Move" action to get the latest address. 
    row['demo_currently_lives_in_address'] = True
    if "VS01" in res or "AS12" in res:
        row['demo_currently_lives_in_address'] = False
    ae08_or_ae09_present = False    
    for result_codes in res.split(','):
      if result_codes == 'AS03':
        row['demo_address_verification_failed'] = True
        break
      if result_codes.startswith('AE'):
        if result_codes == 'AE08' or result_codes == 'AE09':
          ae08_or_ae09_present = True
        else:
          row['demo_address_verification_failed'] = False
          break
    if ae08_or_ae09_present:
      row['demo_address_verification_failed'] = True
    return row

def wait_for_rate_limit():
    time.sleep(1 / REQUESTS_PER_SECOND)

def lookup_property(address, city, state, zip_code, is_mailing_address=True):
    # Check if the property data exists in the database
    query = {
        "address": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "is_mailing_address": is_mailing_address
    }

    existing_data = propertyDataCollection.find_one(query)

    if existing_data and 'api_response' in existing_data:
        # Check if all required fields are present
        flattened_data = flatten_json(existing_data['api_response'].get('Records', [{}])[0])
        missing_fields = [field for field in required_property_api_fields if field not in flattened_data]
        
        if not missing_fields:
            logger.info("All required property data found in database")
            return existing_data['api_response']
        else:
            logger.info(f"Some required fields are missing: {missing_fields}. Calling API for updates.")
    else:
        logger.info("Property data not found in database. Calling API.")
    
    # If not in database or missing fields, proceed with API call
    wait_for_rate_limit()
    
    params = {
        "id": PROPERTY_API_KEY,
        "format": "json",
        "a1": address,
        "city": city,
        "state": state,
        "postal": zip_code,
        "cols": "GrpCurrentDeed,GrpSaleInfo,GrpPrimaryOwner,GrpPropertyUseInfo,GrpParcel,GrpOwnerAddress,GrpTax,GrpBuildingInfo,GrpLotInfo,GrpSalesInfo"
    }
    
    try:
        response = requests.get(PROPERTY_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        
        if response.status_code == 200:
            api_response = response.json()
            
            # Update the existing data or create a new document
            if existing_data:
                existing_data['api_response'] = api_response
                existing_data['timestamp'] = datetime.now(pytz.utc)
                propertyDataCollection.replace_one({"_id": existing_data["_id"]}, existing_data)
            else:
                document = {
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "api_response": api_response,
                    "timestamp": datetime.now(pytz.utc)
                }
                propertyDataCollection.insert_one(document)
            
            return api_response
        elif response.status_code == 429:
            logger.warning("Rate limit exceeded. Waiting before retrying.")
            time.sleep(60)
            return lookup_property(address, city, state, zip_code)
        else:
            logger.error(f"Unexpected status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return None
                
def flatten_json(nested_json, prefix=''):
    flattened = {}
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{prefix}{key}"
            if isinstance(value, dict):
                flattened.update(flatten_json(value, f"{new_key}_"))
            else:
                flattened[new_key] = value
    return flattened

def get_melissa_property_data(df):
    def process_row(row):
        # Use property address if available, otherwise use mailing address
        address = row.get('property_address', '') or row.get('BETTY_UPDATED_ADDRESS_LINE1', '')
        city = row.get('property_city', row.get('mail_city', ''))
        state = row.get('property_state_name_short_code', row.get('mail_state_name_short_code', ''))
        zip_code = row.get('property_zip_code', row.get('mail_zip_code', ''))
        is_mailing_address = not all([row.get('property_address'), row.get('property_city'), 
                                    row.get('property_state_name_short_code'), row.get('property_zip_code')])
        
        property_data = lookup_property(
            address,
            city,
            state,
            zip_code,
            is_mailing_address
        )
        
        if property_data and 'Records' in property_data and property_data['Records']:
            api_data = property_data['Records'][0]
            flattened_data = flatten_json(api_data)
            for field in required_property_api_fields:
                row[f"API_{field}"] = flattened_data.get(field, '')
                
            # Update is_apartment based on PropertyUseGroup
            property_use_group = flattened_data.get('PropertyUseInfo_PropertyUseGroup', '')
            row['is_apartment'] = is_apartment(row.get('property_address', '') or row.get('BETTY_UPDATED_ADDRESS_LINE1', ''), property_use_group)
        else:
            logger.warning(f"No property data found for address: {address}")
            for field in required_property_api_fields:
                row[f"API_{field}"] = ''
        
        return row

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        processed_rows = list(executor.map(process_row, df.to_dict('records')))

    df_updated = pd.DataFrame(processed_rows)
    logger.info(f"Property data lookup complete.")
    return df_updated


def run_all_preprocess(filename,df, data_source, industry_profile):
    newFilePath = filename.split('.csv')[0]+'_appended.csv'
    dist = pgeo.GeoDistance('US')
    
    # Initialize BETTY columns with empty strings
    betty_columns = [
        # Address columns
        "BETTY_UPDATED_ADDRESS_LINE1", "BETTY_UPDATED_ADDRESS_LINE2", 
        "BETTY_UPDATED_ADDRESS_CITY", "BETTY_UPDATED_ADDRESS_STATE", 
        "BETTY_UPDATED_ADDRESS_ZIP",
        # Name columns
        "BETTY_UPDATED_O_1_F_N", "BETTY_UPDATED_O_1_L_N",
        "BETTY_UPDATED_O_2_F_N", "BETTY_UPDATED_O_2_L_N",
        "BETTY_UPDATED_BUSINESS_NAME"
    ]
    for col in betty_columns:
        df[col] = ''
    
    # For roofing industry, prioritize property address and get additional data
    if industry_profile.get('industryType') == 'roofing':
        logger.info("Processing roofing industry data...")
        
        # Map mailing address to property address if property address not available
        if not all(col in df.columns for col in ['property_address', 'property_city', 'property_state_name_short_code', 'property_zip_code']):
            logger.info("Property address fields not found, using mailing address")
            df['property_address'] = df['mail_street_address']
            df['property_city'] = df['mail_city']
            df['property_state_name_short_code'] = df['mail_state_name_short_code']
            df['property_zip_code'] = df['mail_zip_code']
            
        # Set BETTY_UPDATED fields
        df['BETTY_UPDATED_ADDRESS_LINE1'] = df['property_address']
        df['BETTY_UPDATED_ADDRESS_CITY'] = df['property_city']
        df['BETTY_UPDATED_ADDRESS_STATE'] = df['property_state_name_short_code']
        df['BETTY_UPDATED_ADDRESS_ZIP'] = df['property_zip_code']
        logger.info("Using address for roofing industry")
        
        # Get property data from Melissa API
        logger.info("Getting property data from Melissa API...")
        df = get_melissa_property_data(df)
        
        # Commenting this out for now, since we are not using it for the roofing model
        # Get solar potential data using geocode and building insights
        # logger.info("Getting solar potential data...")
        # try:
        #     # First geocode the addresses
        #     df = geocodeFromDataFrame(df)
        #     # Then get building insights using the geocoded coordinates
        #     df = buildingInsightsFromDataFrame(df)
        # except Exception as e:
        #     logger.error(f"Error getting solar potential data: {str(e)}")
    
    # For solar industry, get geocode and building insights
    # elif industry_profile.get('industryType') == 'solar_installer':
    #     df = geocodeFromDataFrame(df)
    #     df = buildingInsightsFromDataFrame(df)
        
    # # For insurance providers, get property data
    # elif industry_profile.get('industryType') == 'insurance_provider':
    #     df = get_melissa_property_data(df)
        
    df['full_name_missing'] = df['owner_full_name'].isna()
    df['age_source'] = ''
    df['potential_age'] = ''
    df['is_business'] = df.apply(lambda row: get_is_business(row), axis=1)
    df['is_public_entity'] = df.apply(lambda row: get_is_public_entity(row), axis=1)
    columns = df.columns
    # calling business API with Melissa
    # try:
    #   df = get_melissa_business_data(df)
    # except Exception as e:
    #   logger.error("hit error fetching data from Melissa", e)
    if "mail_zip_code" in columns:
      df['mail_zip_code']= df.apply(lambda row: handle_zipcode(row['mail_zip_code']), axis = 1)
    if "mail_state_name_short_code" in columns:
        df = df.rename(columns={'mail_state_name_short_code': 'mail_state'})

    columns = df.columns.tolist()  # Define columns from DataFrame

    # 1. Initial name parsing with clean_and_split_owner_names
    logger.info("Starting initial name parsing...")
    
    # Create temporary columns for LLM processing
    df['needs_llm'] = False
    df['temp_owner1_first_name'] = None
    df['temp_owner1_last_name'] = None
    df['temp_owner2_first_name'] = None
    df['temp_owner2_last_name'] = None
    df['temp_business_name'] = None
    df['temp_name_type'] = None
    
    # Process names row by row
    for idx, row in df.iterrows():
        split_names_present = False
        owner_first_name = row.get('owner_first_name')
        owner_last_name = row.get('owner_last_name')
        if pd.notnull(owner_first_name) and pd.notnull(owner_last_name):
            split_names_present = True
        # print('INPUT', row.get('owner_full_name', ''), split_names_present, owner_first_name, owner_last_name)    
        o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm = clean_and_split_owner_names(
            row.get('owner_full_name', ''), 
            split_names_present, 
            owner_first_name, 
            owner_last_name
        )
        # print('OUTPUT', o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm)
        df.at[idx, 'temp_owner1_first_name'] = o_1_f_n
        df.at[idx, 'temp_owner1_last_name'] = o_1_l_n
        df.at[idx, 'temp_owner2_first_name'] = o_2_f_n
        df.at[idx, 'temp_owner2_last_name'] = o_2_l_n
        df.at[idx, 'temp_business_name'] = o_b_n
        df.at[idx, 'temp_name_type'] = name_type
        df.at[idx, 'needs_llm'] = needs_llm
    
    # Before LLM check
    logger.info(f"Number of rows needing LLM: {df['needs_llm'].sum()}")
        
    if df['needs_llm'].any():
        logger.info("Processing complex names with LLM...")
        df = process_names_with_llm(df)
        
        # Only call standardize_name_order_LLM if first and last names were not provided
        if not all(pd.notnull(df['owner_first_name']) & pd.notnull(df['owner_last_name'])):
            logger.info("Standardizing name order for rows without split names...")
            df = standardize_name_order_LLM(df)
    
    # Copy temporary columns to final columns and handle None values
    name_columns = {
        'temp_owner1_first_name': 'BETTY_UPDATED_O_1_F_N',
        'temp_owner1_last_name': 'BETTY_UPDATED_O_1_L_N',
        'temp_owner2_first_name': 'BETTY_UPDATED_O_2_F_N',
        'temp_owner2_last_name': 'BETTY_UPDATED_O_2_L_N',
        'temp_business_name': 'BETTY_UPDATED_BUSINESS_NAME',
        'temp_name_type': 'name_type'
    }
    
    for temp_col, final_col in name_columns.items():
        df[final_col] = df[temp_col].fillna('')  # Convert None to empty string
        df = df.drop(columns=[temp_col])

    # Drop the needs_llm column
    df = df.drop(columns=['needs_llm'])

    # 3. Now proceed with Melissa API calls
    logger.info("Starting Melissa API processing...")
    # Parallelize get_melissa_person_data
    def process_melissa(row):
        return get_melissa_person_data(row)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_melissa, row): idx for idx, row in df.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                updated_row = future.result()
                # Since get_melissa_person_data returns the entire row as a Series
                df.loc[idx, updated_row.index] = updated_row
            except Exception as e:
                logging.error(f"Error processing Melissa data for row {idx}: {e}")

    # Process age with different strategies based on industry type
    def process_age(row):
        # Melissa age should already be set by get_melissa_person_data if available
        if pd.notnull(row['potential_age']) and row['age_source'] == 'Melissa':
            return row['potential_age'], row['age_source']
        
        # For insurance providers, try ED before Agify
        if industry_profile.get('industryType') == 'insurance_provider':
            age, age_source = get_age_ed(row, df)
            if age is not None:
                return age, age_source
            
        # For all industries, try Agify as final fallback
        age, age_source = get_age_agify(row, df)
        return age, age_source if age_source else 'Unknown'

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_age, row): idx for idx, row in df.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                age, age_source = future.result()
                df.at[idx, 'potential_age'] = age
                df.at[idx, 'age_source'] = age_source
            except Exception as e:
                logger.error(f"Error processing age for row {idx}: {e}")
                df.at[idx, 'potential_age'] = None
                df.at[idx, 'age_source'] = 'Unknown'
                
    logger.info(f"Age and source after processing: {df[['potential_age', 'age_source']].dropna()}")
    
    df['is_po_box'] = df['BETTY_UPDATED_ADDRESS_LINE1'].str.lower().str.contains('po box', na=False)
    df['is_apartment']= df.apply(lambda row: is_apartment(row['BETTY_UPDATED_ADDRESS_LINE1']), axis = 1)
    # df['potential_age'], df['age_source'] = zip(*df.apply(lambda row: get_approximate_age(row, df), axis=1))
    # logging.info(f"Age and source after get_melissa_person_data: {df[['potential_age', 'age_source']].dropna()}")
    # if df['potential_age'].isnull().sum() == df.shape[0]: 
    #   df['potential_age'] = 0
    # TODO: Multiple threads for industry type of file upload
    # For solar 
    print("industry_profile", industry_profile['industryType'])
    if industry_profile['industryType'] == 'solar_installer':
      df = geocodeFromDataFrame(df)
      print("after geocodeFromDataFrame")
      df = buildingInsightsFromDataFrame(df)      
      print("after buildingInsightsFromDataFrame")

    if industry_profile['industryType'] == 'insurance_provider':
        df = get_melissa_property_data(df)
      
    if industry_profile['industryType'] == 'real_estate_investors':
      if "property_zip_code" in columns:
        s = pd.Series(df["property_zip_code"])
        try:
          s = s.interpolate(method='nearest')
        except:
          s = s.interpolate(method='linear')
        df["property_zip_code"] = s.values
        df["property_zip_code"]= df.apply(lambda row: handle_zipcode(row["property_zip_code"]), axis = 1)
        
      df['dist_btw_site_mail_zip'] = df.apply(lambda row: get_dist_btw_site_and_mail_zip(row, dist), axis = 1)
    
      if "owner_1_type" in columns:
        df['owner_1_type'] = df['owner_1_type'].fillna(1)
        frequent_owner_1_type = df["owner_1_type"].mode().iat[0] #gets the maximum frquency element
        df["owner_1_type"]= df.apply(lambda row: handle_owner_type(frequent_owner_1_type,row), axis = 1)

      if 'market_price_per_acre' in columns:
        df = change_type(df, ['market_price_per_acre'], columns)
        df["market_price"] = df['market_price_per_acre'] * df['lot_acreage']

      if "property_vacant" in columns:
        df['property_vacant'] = transform_to_boolean(df['property_vacant'], False)

      if 'property_address' in df.columns and 'mail_street_address' in df.columns:
        df['is_address_same'] = df['property_address'].str.lower() == df['mail_street_address'].str.lower()

    if "do_not_mail" in columns:
      df['do_not_mail'] = df['do_not_mail'].fillna(0)
      frequent_do_not_mail = df["do_not_mail"].mode().iat[0] #gets the maximum frquency element
      df["do_not_mail"] = df.apply(lambda row: handle_do_not_mail(frequent_do_not_mail, row), axis = 1)

    if "phone1" in columns:
      df['mobile_number_present'] = df.apply(lambda row:mobile_number_present(row), axis = 1)
     
    if "mail_state" in columns:
      df = df.rename(columns={'mail_state':'mail_state_name_short'})
      
    df = convert_to_date(df,columns)
    #column_names = ["property_tax","tax_year","offer_price","market_price","delinquent_tax_year","delinquent_tax_value", "last_sale_price", "age"]
    column_names = ["lot_area_sqft", "property_tax","tax_year","offer_price","market_price","delinquent_tax_year",
                    "delinquent_tax_value", "last_sale_price", "age", "num_bathrooms", "num_bedrooms", "living_area_sqft",
                      "building_area_sqft", "num_of_stories", "sqft_price","owner_num_total_properties"]
    df = change_type(df,column_names,columns)

    if 'owner_first_name' in df.columns and 'owner_2_first_name' in df.columns:
      df['num_owners'] = df[['owner_first_name', 'owner_2_first_name']].apply(lambda row: sum(pd.notnull(row) & (row != '')), axis=1)
      
    df = change_type(df, ['year_built'], columns)
    df['age_source'] = df.apply(lambda row: row['age_source'] if row['age_source'] else 'Unknown', axis=1)
    logger.info(f"Final DataFrame check before saving: {df[['potential_age', 'age_source']].dropna()}")

    return newFilePath, df
    

# Script Starts Here
if __name__ == '__main__':
  try:
    industry_profile = {}
    data_source=""

    for i,arg in enumerate(sys.argv):
        if arg == '--file_path':
            file_path = sys.argv[i+1]
            OrignalFilePath = file_path  # Assign here
        if arg == "--data_source":
            data_source = sys.argv[i+1]
        if arg == "--industry_profile":
            industry_profile = json.loads(sys.argv[i+1])

    OrignalFilePath = file_path
    newFilePath = ""
    df = pd.read_csv(OrignalFilePath, low_memory=False, on_bad_lines='warn')

    if(len(sys.argv) < 1):
      logger.error("Insufficient Arguments to main function")
    else:
      newFilePath,df_new = run_all_preprocess(file_path,df, data_source, industry_profile)
      df_new.to_csv(newFilePath,index=False)
      logger.info({"success":"True","OrignalFilePath":OrignalFilePath,"newFilePath":newFilePath})
  except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    logger.error({"success":"False","OrignalFilePath":OrignalFilePath,"newFilePath":newFilePath,"error": "preprocessing_failure", "error_details": exc})

# TODO:
# owner_occupied is boolean
# is_public_entity is boolean
# flood_zone_code is those letters A, AE, etc. all upper case
# standardize last_sale_document_type enums
