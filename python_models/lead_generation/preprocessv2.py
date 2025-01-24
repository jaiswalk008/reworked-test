"""
HOW TO RUN THIS FILE:
1. Place the input file in the 'data' folder with an extension of 'column-mappingv2.csv'.
2. Navigate outside the 'data' folder and run this file using the following command:
    python .\preprocessv2.py --file_path "file.xlsx"
3. The CSV output file will be generated inside the 'data' folder with a suffix of 'preprocessv2.csv'.  
"""

import sys
sys.path.insert(1, 'python_models')
import pandas as pd
import re
import numpy as np
import os
import requests
import time
import logging
import argparse
from datetime import datetime
from utils import select_data
import json

logging.basicConfig(level=logging.INFO, format='%(message)s')


def append_data(df, extra_columns=[]):
    df.reset_index(inplace=True, drop=True)
    logging.info("   10.1 Creating melissa URL.... ")
    melissa_key='cKEf_4HZGh_L5EV98iXjXC**'
    melissa_url = f"https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify?t=&id={melissa_key}"
    required_columns=["DateOfBirth", "Education", "Gender", "HouseholdIncome"]
    if extra_columns:
        required_columns = required_columns + extra_columns
        
    required_columns=','.join(required_columns)
    melissa_url+=f"&cols={required_columns}"
    logging.info(f"   10.2 URL creation completed, fetching data now.... URL - {melissa_url}")
    results = []
    start_time = time.time()
    
    # NOTE: while testing add '.head(50)' after df to send only 50 requests to melissa api
    # for index, row in df.head(2).iterrows():
    
    for index, row in df.iterrows():
        complete_url = melissa_url
        if pd.notna(row['full_name']):
            complete_url+=f"&full={row['full_name']}"
        if pd.notna(row['zipcode']):
            complete_url+=f"&postal={row['zipcode']}"
        if pd.notna(row['full_address']):
            complete_url+=f"&a1={row['full_address']}"
        if pd.notna(row['city']):
            complete_url+=f"&city={row['city']}"
        if "state" in row and pd.notna(row['state']):
            complete_url+=f"&state={row['state']}"
        if 'email_address' in row and pd.notna(row['email_address']):
            complete_url+=f"&email={row['email_address']}"
        complete_url+="&format=json"
        complete_url = re.sub('\s+', ' ', complete_url)
        complete_url = complete_url.replace(' ', '%20')
        try:
            response = requests.request('GET', url=complete_url, timeout=5)
            if response.status_code == 200 and response.headers["content-type"].strip().startswith("application/json"):
                result_data = response.json()
                results.append(result_data.get('Records'))

        except:
            logging.error(f"   10.e ERROR!! | row index -> {index} | response text -> {response.text}.... ")
            pass 
            
        if index!=0 and index%500==0:
            time_taken = round((time.time() - start_time)/60,2)
            logging.info(f"   10.3 Completed fetching {index} rows, time taken -> {time_taken} minutes, total estimated time remaining -> {round((time_taken//500*(df.shape[0]-index))/60,2)+1} minutes....")
            start_time = time.time()
    logging.info("   10.4 Fetching data completed.... ")
    results = [val for val in results if val]
    result_df = pd.DataFrame(results)
    result_df = result_df[0].apply(pd.Series)
    result_df.columns = [col.lower() for col in result_df.columns]
    result_df.replace(' ', np.nan, inplace=True)
    logging.info("   10.5 Created new dataframe fetched data.... \n")
    return result_df


def calculate_age(df, column_name):
    # Convert 'YYYYMM' to datetime
    df[column_name] = pd.to_datetime(df[column_name], format='%Y%m')
    
    # Calculate age
    now = pd.Timestamp('now')
    df['age'] = (now.year - df[column_name].dt.year) - ((now.month - df[column_name].dt.month) < 0)
    
    del df[column_name]
    return df


try:
    logging.info("1. preprocessv2 code started.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Specify a CSV filename ending with -columnmappingv2, e.g., "filename-columnmappingv2.csv"')
    parser.add_argument('--max_rows', type=int, required=False, help='Specify the maximum number of rows required to create a model, e.g., 1000')

    parser.add_argument('--feature_columns', metavar='S', type=str, nargs='+', required=False,
                        help='Example: {"feature_columns": ["name", "address"]}')
    parser.add_argument('--data_type', metavar='S', type=str, nargs='+', required=False,
                        help='Specify the data type if it is for train or test')
    parser.add_argument('--industry_profile', metavar='S', type=str, nargs='+', required=False,
                        help='Specify industry profile')
    args = parser.parse_args()
    industry_profile = {}
    feature_columns =[]
    
    if args.feature_columns: 
        feature_columns = json.loads(args.feature_columns[0])
        print("feature columns preprocess", feature_columns)
        feature_columns = feature_columns["feature_columns"]

    if args.industry_profile: 
        industry_profile = json.loads(args.industry_profile[0])
        print("industry_profile preprocess", industry_profile)

    data_type = "train"
    if args.data_type: data_type = args.data_type[0]

    file_path = args.file_path #rf'Berkling Tools Final File 10.904-contact-append_column-mappingv2.csv'
    
    # Add the '-prepocessv2' suffix to the file name
    preprocess_file_path = file_path.split('.csv')[0]+'-preprocessv2.csv'

    input_df = pd.read_csv(file_path)
    logging.info(f"2. Reading column mapping file.... FILE - {file_path} \n")

    print("args.max_rows", args.max_rows)
    input_df = select_data(input_df, args.max_rows)
    logging.info("10. Starting append_data function.... ")
    append_df = append_data(input_df)
    logging.info(f"11. append_data function completed.... {input_df.shape} \n")
    logging.info("12. Creating age from date of birth column....  \n")
    append_df = calculate_age(append_df, 'dateofbirth')
    logging.info("13. calculate_age function completed....  \n")
    
    # Save the DataFrame to a csv file with the new name
    append_df.to_csv(preprocess_file_path, index=False)
    logging.info({"success":"True", "OriginalFilePath": file_path, "NewFilePath": preprocess_file_path})
        
except Exception as e:
    exc = str(e).replace('"','').replace("'",'')
    logging.error({"success":"False", "OriginalFilePath": file_path, "NewFilePath": preprocess_file_path, "error": "preprocess_failure", "error_details": exc})
