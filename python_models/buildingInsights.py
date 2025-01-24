
import requests
import pandas as pd
from os.path import basename, dirname, join
from tqdm import tqdm 
import logging
import pymongo
import datetime
import os
import sys
import json
import time
from dotenv import load_dotenv
load_dotenv()
dbclient = pymongo.MongoClient(os.getenv('MONGO_URL'))
mydb= dbclient['myFirstDatabase']

solarInsights = mydb["solarInsights"]
logging.basicConfig(level=logging.INFO, format="%(message)s")
building_insights_api_url = 'https://solar.googleapis.com/v1/buildingInsights:findClosest'

# Add API key here
BUILDING_INSIGHTS_KEY = os.getenv('G_API_QI')

# Function to find the default bill
def find_default_bill(data):
    if data and len(data) > 0:
        for analysis in data: 
            if analysis.get("defaultBill") == True:
                return analysis
    return {}


def unmarshalJSON(response: dict) -> pd.Series:
    """
    Traverses JSON response of buildingInsights API recursively and converts into a pd.Series object

    Parameters: 
    response (dict): Output of the buildingInsights API

    Returns:
    pd.Series: The output of buildingInsights API in pd.Series format    
    """
    
    stack = [((), response)]
    result = {}

    while stack:
        keys, node = stack.pop()

        if isinstance(node, dict):
            for k, v in node.items():
                stack.append((keys + (k,), v))
        elif isinstance(node, list):
            for i, v in enumerate(node):
                stack.append((keys + (str(i),), v))
        else:
            result['_'.join(keys)] = node

    return pd.Series(result)


def buildingInsights(latitude: float, longitude: float):
    """
    Pings buildingInsights API 

    Parameters:
    latitude (float): Latitude of address
    longitude (float): Longitude of address

    Returns:
    dict: Response of the API call if response code is 200 OK. Else returns None
    int: Status code of the API call.

    NOTE: Set `requiredQuality` parameter to LOW; higher quality is returned if available.
    """
    # Check if latitude and longitude are valid
    if latitude is None or longitude is None or pd.isna(latitude) or pd.isna(longitude):
        logging.info("Invalid latitude or longitude: %s, %s", latitude, longitude)
        return "Invalid latitude or longitude.", 400
    

    findObj = {
        "location.latitude": latitude,
        "location.longitude": longitude,
        "requiredQuality": "LOW",
    }
    if solarInsights.count_documents({"params": findObj }) > 0:
        logging.info("Building insights found from db")
        response = solarInsights.find_one({"params": findObj})
    else:
        params = {
            "location.latitude": latitude,
            "location.longitude": longitude,
            "requiredQuality": "LOW",
            "key": BUILDING_INSIGHTS_KEY
        }
        max_retries = 5 
        backoff_time = 1  # Initial backoff time in seconds
        for attempt in range(max_retries):
            response = requests.get(building_insights_api_url, params=params, timeout=5)
            if response.status_code == 200:
                logging.info("Building insights API called successfully with status code")
                response = response.json()
                response['params'] = findObj
                response['created_at'] = datetime.datetime.now()
                x = solarInsights.insert_one(response)
                break
            elif response.status_code == 429:
                logging.error("Rate limit in building insights, error blocking for 2 sec")
                time.sleep(backoff_time)
                backoff_time *= 2
            else:
                logging.info(f"Calling building insights API with object: {findObj}")
                logging.error("Error while calling builing insights %s", response)
                return response.json(), response.status_code
        else:
            return None, response.status_code
    # Extract financial analyses data
    financialAnalyses = {}
    if "financialAnalyses" in response.get("solarPotential", {}):
        financialAnalyses = find_default_bill(response["solarPotential"]["financialAnalyses"])
    
    # Extract needed values
    needed_values = {
        'name': response.get('name', ''),
        'latitude': response.get('center', {}).get('latitude', ''),
        'longitude': response.get('center', {}).get('longitude', ''),
        'solarPotential.maxArrayPanelsCount': response.get('solarPotential', {}).get('maxArrayPanelsCount', ''),
        'solarPotential.maxArrayAreaMeters2': response.get('solarPotential', {}).get('maxArrayAreaMeters2', ''),
        'solarPotential.maxSunshineHoursPerYear': response.get('solarPotential', {}).get('maxSunshineHoursPerYear', ''),
        'solarPotential.maxSunshineHoursPerYear': response.get('solarPotential', {}).get('maxSunshineHoursPerYear', ''),
        'solarPotential.wholeRoofStats.areaMeters2': response.get('solarPotential', {}).get('wholeRoofStats', {}).get('areaMeters2', ''),
        'solarPotential.wholeRoofStats.areaMeters2': response.get('solarPotential', {}).get('wholeRoofStats', {}).get('areaMeters2', ''),
        'solarPotential.roofSegmentStats': len(response.get('solarPotential', {}).get('roofSegmentStats', [])),
        'remainingLifetimeUtilityBill': financialAnalyses.get('financialDetails', {}).get('remainingLifetimeUtilityBill', {}).get('units', ''),
        'federalIncentive': financialAnalyses.get('financialDetails', {}).get('federalIncentive', {}).get('units', ''),
        'solarPotential.financialAnalyses.financialDetails.remainingLifetimeUtilityBill': financialAnalyses.get('financialDetails', {}).get('remainingLifetimeUtilityBill', {}).get('units', ''),
        'solarPotential.financialAnalyses.financialDetails.federalIncentive': financialAnalyses.get('financialDetails', {}).get('federalIncentive', {}).get('units', ''),
        'solarPotential.financialAnalyses.financialDetails.stateIncentive': financialAnalyses.get('financialDetails', {}).get('stateIncentive', {}).get('units', 'None'),
        'solarPotential.financialAnalyses.financialDetails.utilityIncentive': financialAnalyses.get('financialDetails', {}).get('utilityIncentive', {}).get('units', 'None'),
        'solarPotential.financialAnalyses.financialDetails.costOfElectricityWithoutSolar': financialAnalyses.get('financialDetails', {}).get('costOfElectricityWithoutSolar', {}).get('units', 'None'),
        'solarPotential.financialAnalyses.financialDetails.netMeteringAllowed': financialAnalyses.get('financialDetails', {}).get('netMeteringAllowed', 0),
        'solarPotential.financialAnalyses.financialDetails.solarPercentage': financialAnalyses.get('financialDetails', {}).get('solarPercentage', 0),
        'solarPotential.financialAnalyses.financialDetails.percentageExportedToGrid': financialAnalyses.get('financialDetails', {}).get('percentageExportedToGrid', ''),
        'solarPotential.financialAnalyses.financialDetails.percentageExportedToGrid': financialAnalyses.get('financialDetails', {}).get('percentageExportedToGrid', ''),
        'solarPotential.financialAnalyses.leasingSavings.leasesAllowed': financialAnalyses.get('leasingSavings', {}).get('leasesAllowed', ''),
        'solarPotential.financialAnalyses.leasingSavings.leasesSupported': financialAnalyses.get('leasingSavings', {}).get('leasesSupported', ''),
        'solarPotential.financialAnalyses.leasingSavings.annualLeasingCost': financialAnalyses.get('leasingSavings', {}).get('annualLeasingCost', {}).get('units', ''),
        'solarPotential.financialAnalyses.leasingSavings.savings.savingsYear1': financialAnalyses.get('leasingSavings', {}).get('savings', {}).get('savingsYear1', {}).get('units', ''),
        'solarPotential.financialAnalyses.leasingSavings.savings.savingsYear20': financialAnalyses.get('leasingSavings', {}).get('savings', {}).get('savingsYear20', {}).get('units', ''),
        'solarPotential.financialAnalyses.leasingSavings.savings.savingsLifetime': financialAnalyses.get('leasingSavings', {}).get('savings', {}).get('savingsLifetime', {}).get('units', ''),
        'solarPotential.financialAnalyses.leasingSavings.savings.savingsLifetime': financialAnalyses.get('leasingSavings', {}).get('savings', {}).get('savingsLifetime', {}).get('units', ''),
        'solarPotential.financialAnalyses.cashPurchaseSavings.outOfPocketCost': financialAnalyses.get('cashPurchaseSavings', {}).get('outOfPocketCost', {}).get('units', ''),
        'solarPotential.financialAnalyses.cashPurchaseSavings.paybackYears': financialAnalyses.get('cashPurchaseSavings', {}).get('paybackYears', ''),
        'solarPotential.financialAnalyses.cashPurchaseSavings.paybackYears': financialAnalyses.get('cashPurchaseSavings', {}).get('paybackYears', ''),
    }

    return needed_values, 200

# def buildingInsightsFromDataFrame(filename, data: pd.DataFrame, failed_indices=None) -> None:
def buildingInsightsFromDataFrame(data: pd.DataFrame):
    
    """
    Calls buildingInsights API on a DataFrame containing coordinates of the desired addresses. 
    Saves new file in the same folder as the original CSV file

    Parameters:
    data (pd.DataFrame): DataFrame containing the coordinates and other necessary information.

    NOTE: Ensure the DataFrame contains coordinates in the columns `geocode_latitude` for latitude and `geocode_longitude` for longitude.
    """
    try:
        n = len(data.index)
        building_insights_error = []
        with open('errors.csv', 'w') as error:
            
            error.write('index, success, cause, status\n')
            for index, row in tqdm(data.iterrows(), total=n):
                try:
                    response, status = None, 200
                    response, status = buildingInsights(row['geocode_latitude'], row['geocode_longitude'])
                    if status == 200:
                        for key, value in response.items():
                            data.at[index, key] = value

                        error.write('{}, {}, {}, {}\n'.format(index, "True", None, status))
                        logging.info(f"Successfully processed row {index} with status code ${status}")
                        building_insights_error.append(None)
                    else:
                        error.write('{}, {}, {}, {}\n'.format(index, "False", "API Error", status))
                        logging.error(f"Error processing row in else building insights {index}: API Error with status code {status}")
                        building_insights_error.append(response)
                except Exception as e:
                    logging.error(f"Error processing row in exception building insights {index}: {e}")
                    error.write('{}, {}, {}, {}\n'.format(index, "False", e.__class__, status))
                    building_insights_error.append(str(e))

        if len(building_insights_error) == n:
                data['building_insights_error'] = building_insights_error

        # Remove the 'buildingInsightsResponse' column if it exists
        if 'buildingInsightsResponse' in data.columns:
            data.drop(columns=['buildingInsightsResponse'], inplace=True)
    
        return data
        
    except Exception as ex:
        logging.error(f"An error occurred in building Insights.py: {ex}")
        return data

# logging.basicConfig(level=logging.INFO, format="%(message)s")
# OrignalFilePath =''
# newFilePath=''
# try:
#   if(len(sys.argv) < 1):
#     logging.error("Insufficient Arguments to main function")

#   for i,arg in enumerate(sys.argv):
#       if arg == '--file_path':

#   OrignalFilePath = file_path
#   newFilePath = ""
#   data = pd.read_csv(OrignalFilePath, low_memory=False, on_bad_lines='warn')
#   df_new = buildingInsightsFromDataFrame(data)
#   df_new.to_csv(OrignalFilePath,index=False)
#   logging.info({"success":"True","OrignalFilePath":OrignalFilePath,"newFilePath":OrignalFilePath})
  
# except Exception as e:
#   exc = str(e).replace('"','')
#   exc = str(exc).replace("'",'')
#   logging.error({"success":"False","OrignalFilePath":OrignalFilePath,"newFilePath":newFilePath,"error": "building_insights_failure", "error_details": exc})
