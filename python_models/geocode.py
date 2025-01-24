import pandas as pd
import requests
import os
from tqdm import tqdm
import pymongo
import numpy as np
import logging
import datetime
import time
from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
GEOCODE_KEY = os.getenv('G_API_QI')
dbclient = pymongo.MongoClient(os.getenv('MONGO_URL'))
mydb= dbclient['myFirstDatabase']

geocodeInsights = mydb["geocodeInsights"]

geocode_api_url = 'https://maps.googleapis.com/maps/api/geocode/json'
def concatAddress(data: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a column named fullAddress in the DataFrame. New column contains the full address of the record.
    """
    fullAddresses = []
    
    for _, row in data.iterrows():
        fullAddress = '{}, {}, {}, {}'.format(row['BETTY_UPDATED_ADDRESS_LINE1'], row['BETTY_UPDATED_ADDRESS_CITY'], row['BETTY_UPDATED_ADDRESS_STATE'], str(row['BETTY_UPDATED_ADDRESS_ZIP']))
        fullAddresses.append(fullAddress)

    data['fullAddress'] = fullAddresses
    return data

def geocoder(address: str):
    """
    Calls Google Geocode API on a single address
    
    Parameters:
    address (str): The address to be geocoded.

    Returns:
    tuple: JSON response and status code from the API call.
    """
    
    max_retries = 5
    backoff_time = 1
    findObj = {
        "address": address,
    }
    if geocodeInsights.count_documents({"params": findObj }) > 0:
        logging.info("Geocode found from db")
        response = geocodeInsights.find_one({"params": findObj})
        return response, 200
    else:
        params = {
            'address': address,
            'key': GEOCODE_KEY
        }   
        for attempt in range(max_retries):
            
            response = requests.get(geocode_api_url, params=params)
            if response.status_code == 200:
                logging.info("Geocode Api called successfullt with status code")
                response_data = response.json()
                response_data['params'] = findObj
                response_data['created_at'] = datetime.datetime.now()
                x = geocodeInsights.insert_one(response_data)
                return response_data, response.status_code
            elif response.status_code == 429:
                logging.error("Rate limit reached, retrying in", backoff_time, "seconds")
                time.sleep(backoff_time)
                backoff_time *= 2
            else:
                logging.error("Error calling geocode API:", response)
                return response, response.status_code

        # If all retries fail
        logging.error("Failed after", max_retries, "attempts", "params", params)
        return None, response.status_code

    

def geocodeFromDataFrame(data: pd.DataFrame, last_processed_index=0) -> pd.DataFrame:
    """
    Calls geocode API on a DataFrame containing addresses
    """
    try:
        n = len(data.index)
        geocoderResponse = []
        geocode_errors = []
        data = concatAddress(data)
        with open('errors.csv', 'w') as errors:
            errors.write('index, success, cause, status_code\n')
            for index, row in tqdm(data.iterrows(), total=n, initial=last_processed_index):
                if index < last_processed_index:
                    continue  # Skip already processed records
                if not pd.isna(row['fullAddress']):
                    try:
                        response, status = geocoder(address=row['fullAddress'])
                        if status == 200:
                            errors.write('{}, True, None, {}\n'.format(index, status))
                            if response['status'] == 'OK':
                                geocoderResponse.append(response)  # Append response to the list
                                geocode_errors.append(None)
                            else: 
                                logging.error(f"Error processing row in else geocode {index}: Empty result with status code {status}", response)
                                geocoderResponse.append(None)  # Append None for unsuccessful responses
                                geocode_errors.append('Empty result with status code')    
                        else: 
                            errors.write('{}, {}, {}, {}\n'.format(index, "False", "API Error", status))
                            logging.error(f"Error processing row in else geocode {index}: API Error with status code {status}")
                            geocoderResponse.append(None)  # Append None for unsuccessful responses
                            geocode_errors.append(response)
                        
                    except Exception as e:
                        geocoderResponse.append(None)  # Append None for unsuccessful responses
                        logging.error(f"Error processing row in exception geocode {index}: {e}")
                        errors.write('{}, False, {}, None\n'.format(index, e.__class__))
                        geocode_errors.append(str(e))
                else:
                    logging.error(f"NaN in Address geocode {index}: {e}")
                    geocoderResponse.append(None)  # Append None for empty addresses
                    errors.write('{}, False, NaN in Address, None\n'.format(index))
                    geocode_errors.append('NaN in Address')
        # Ensure that the length of geocoderResponse matches the length of the DataFrame
        if len(geocode_errors) == n:
            data['geocode_error'] = geocode_errors
        if len(geocoderResponse) == n:
            data['geocodeResponse'] = geocoderResponse
        else:
            logging.error("Length of geocoderResponse does not match length of DataFrame.")
        data = makeLatlongs(data)
        if 'geocodeResponse' in data.columns:
            data.drop(columns=['geocodeResponse'], inplace=True)
        return data
     
    except Exception as ex:
        logging.error(f"An error occurred from geocode.py: {ex}")
        return data

def makeLatlongs(data: pd.DataFrame):
    """
    Extracts latitude and longitude from geocode API response
    """
    lats, longs, formattedAddresses = [], [], []
    n = len(data.index)

    for index, row in tqdm(data.iterrows(), total=n):
        try:
            response = row['geocodeResponse']
            lat = response['results'][0]['geometry']['location']['lat']
            lng = response['results'][0]['geometry']['location']['lng']
            lats.append(lat)
            longs.append(lng)
            formattedAddresses.append(response['results'][0]['formatted_address'])
        except Exception as e:
            lats.append(None)
            longs.append(None)
            formattedAddresses.append(None)
    data['geocode_latitude'] = lats
    data['geocode_longitude'] = longs
    data['formatted_address'] = formattedAddresses
    return data


# logging.basicConfig(level=logging.INFO, format="%(message)s")
# OrignalFilePath =''
# newFilePath=''
# try:
#   if(len(sys.argv) < 1):
#     logging.error("Insufficient Arguments to main function")

#   for i,arg in enumerate(sys.argv):
#       if arg == '--file_path':
#           file_path = sys.argv[i+1]

#   OrignalFilePath = file_path
#   newFilePath = ""
#   data = pd.read_csv(OrignalFilePath, low_memory=False, on_bad_lines='warn')
  
#   # to get lat long
#   df_new = geocodeFromDataFrame(data)
  
#   df_new.to_csv(OrignalFilePath,index=False)
#   logging.info({"success":"True","OrignalFilePath":OrignalFilePath,"newFilePath":OrignalFilePath})
  
# except Exception as e:
#   exc = str(e).replace('"','')
#   exc = str(exc).replace("'",'')
#   logging.error({"success":"False","OrignalFilePath":OrignalFilePath,"newFilePath":newFilePath,"error": "geocode_failure", "error_details": exc})
