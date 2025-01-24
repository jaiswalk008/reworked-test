import sys
sys.path.insert(1, 'python_models')

import pandas as pd
from utils import create_columns_if_doesnt_exist, required_columns_exist, clean_name, clean_email, read_file, basic_preprocessing, handle_zipcode, preprocess_data
import argparse
import logging
import os
import json
logging.basicConfig(level=logging.INFO, format='%(message)s')

rowCount = 0
try:
    logging.info("1. Starting column_mapping code.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Please add a CSV filename, eg - filename.csv')

    #TODOO: it should accept null feature column array

    # Use nargs='+' to gather one or more values for the 'strings' argument
    parser.add_argument('--feature_columns', metavar='S', type=str, nargs='+', required=False,
                        help='example {"feature_columns": ["name", "address"]}')

    parser.add_argument('--data_type', metavar='S', type=str, nargs='+', required=False,
                        help='Please add data type if its for train or test')

    parser.add_argument('--industry_profile', metavar='S', type=str, nargs='+', required=False,
                        help='Please industry profile')

    args = parser.parse_args()

    industry_profile = {}
    module_type = "v1.1"
    if args.industry_profile: 
        industry_profile = json.loads(args.industry_profile[0])
        print("industry_profile", industry_profile)
    data_type = "train"
    if args.data_type: data_type = args.data_type[0]

    file_path = args.file_path
    base_file_path = file_path.rsplit('.', 1)[0]
    # print(args.feature_columns)
    if args.feature_columns: 
        feature_columns = json.loads(args.feature_columns[0])
        print("feature_columns", feature_columns)
        feature_columns = feature_columns["feature_columns"]
    else: feature_columns = []
    # if args.feature_columns: feature_columns = args.feature_columns
    # print("feature columns", feature_columns)
    # feature_columns = ["full_name"]
    logging.info(f"* Loading Data.... file_path = {file_path}, feature_columns = {feature_columns} \n")

    df = read_file(file_path)
    rowCount = df.shape[0]
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))
    logging.info(f"* Reading file path.... {df.shape}\n")
    # df = clean_data(df)
    df = basic_preprocessing(df, data_type)
    logging.info(f"* after basic_preprocessing function\n")
    df = create_columns_if_doesnt_exist(df) 
    logging.info(f"* Finishing create_columns_if_doesnt_exist function\n")
    df = required_columns_exist(df, industry_profile)
    logging.info(f"* Basic Preprocessing, column creation and checking completed.... {df.shape}\n")
    logging.info("* Running clean_email function....  \n")
    if "email_address" in df.columns:
        df = clean_email(df)
    logging.info(f"* Clean_email function completed.... {df.shape} \n")
    logging.info(f"* Running clean_name function....  \n")
    df = clean_name(df)
    logging.info(f"* Clean_name function completed.... {df.shape} \n")
    logging.info(f"* Running clean_zipcode function....  \n")
    df['zipcode']= df.apply(lambda row: handle_zipcode(row['zipcode']), axis = 1)
    logging.info(f"* clean_zipcode function completed.... {df.shape} \n")
    logging.info(f"* Starting data append process....\n")

    # df = df[feature_columns]
    logging.info(f"* Preprocessing train data.... {df.shape}, feature_columns = {feature_columns} \n")
    df = preprocess_data(df, feature_columns)

    df.to_csv(f'{base_file_path}_rwr.csv', index=False)
    logging.info({"success":"True","OriginalFilePath": file_path,"feature_columns": feature_columns, "NewFile": "_rwr.csv", "rowCount": rowCount})
       
except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    pass
    logging.error({"success":"False","OriginalFilePath": file_path, "error": "column_mapping_failure", "error_details": exc, "rowCount": rowCount})
