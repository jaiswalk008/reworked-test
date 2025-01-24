import sys
sys.path.insert(1, 'python_models')

import pandas as pd
from utils import read_file, append_zipcode_data, create_derived_columns, append_data, handle_zipcode
import argparse
import logging
import os
import json

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    #TODOO: should accept empty feature array
    #TODOO call select_data to select random 1000 records
    logging.info("1. Starting preprocess code.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Please add a CSV filename, eg - filename.csv')
    parser.add_argument('--feature_columns', metavar='S', type=str, nargs='+', required=False,
                        help='example {"feature_columns": ["name", "address"]}')
    parser.add_argument('--max_rows', type=int, required=False, help='Max number of rows required to create a model, eg - 1000')
    parser.add_argument('--data_type', metavar='S', type=str, nargs='+', required=False,
                        help='Please add data type if its for train or test')
    parser.add_argument('--industry_profile', metavar='S', type=str, nargs='+', required=False,
                        help='Please industry profile')
    industry_profile = {}
    args = parser.parse_args()

    if args.feature_columns: 
        feature_columns = json.loads(args.feature_columns[0])
        print("feature columns preprocesspy", feature_columns)
        feature_columns = feature_columns["feature_columns"]
    industry_profile = {}

    if args.industry_profile: 
        industry_profile = json.loads(args.industry_profile[0])
        print("industry_profile preprocesspy", industry_profile)

    data_type = "train"
    if args.data_type: data_type = args.data_type[0]

    file_path = args.file_path
    base_file_path = file_path.rsplit('.', 1)[0]

    df = read_file(file_path)
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))
    us_zips_file_path = os.path.join(python_scripts_path, "uszips.csv")
    logging.info("us_zips_file_pathus_zips_file_path {us_zips_file_path}\n" )
    logging.info(us_zips_file_path)
    uszipcodes = pd.read_csv(us_zips_file_path)

    logging.info(f"* Starting data append process....\n")
    # required_columns = ['num_missing_in_row', 'is_name_missing', 'is_business']

    df, required_columns = append_data(df)
    df['zipcode']= df.apply(lambda row: handle_zipcode(row['zipcode']), axis = 1)
    logging.info("Before append_zipcode_data")
    df = append_zipcode_data(df, uszipcodes )
        
    logging.info(f"* Creating derived columns.... {df.shape} \n")
    df = create_derived_columns(df)
    logging.info(f"* Derived columns created.... {df.shape} \n")

    df.to_csv(f'{base_file_path}_appended.csv', index=False)
    logging.info({"success":"True","OriginalFilePath": file_path, "NewFile": "rwr_appended.csv"})
    
except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    pass
    logging.error({"success":"False","OriginalFilePath": file_path, "error": "preprocess", "error_details": exc})

