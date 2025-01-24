"""
HOW TO RUN THIS FILE:
1. Create a 'data' folder and place the raw input file from the client inside it.
2. Navigate outside the 'data' folder and run this file using the following command:
    python .\column_mappingv2.py --file_path "file.xlsx"
3. The CSV output file will be generated inside the 'data' folder with a suffix of 'column-mappingv2.csv'.
"""

import sys
sys.path.insert(1, 'python_models')
import pandas as pd
import logging
import argparse
import json
from utils import create_columns_if_doesnt_exist, required_columns_exist, clean_name, clean_email, read_file, basic_preprocessing, handle_zipcode
logging.basicConfig(level=logging.INFO, format='%(message)s')
from utils import create_columns_if_doesnt_exist, required_columns_exist

try:
    logging.info("1. Initiating column_mappingv2 code.... \n")
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Specify the CSV filename, e.g., "filename.csv"')
    parser.add_argument('--industry_profile', metavar='S', type=str, nargs='+', required=False,
                        help='Specify industry profile')
    parser.add_argument('--feature_columns', metavar='S', type=str, nargs='+', required=False,
                        help='Example: {"feature_columns": ["name", "address"]}')
    args = parser.parse_args()

    # Extracting input parameters
    file_path = args.file_path
    module_type = "v2"
    industry_profile = json.loads(args.industry_profile[0]) if args.industry_profile else {}
    feature_columns = json.loads(args.feature_columns[0])["feature_columns"] if args.feature_columns else []

    logging.info(f"2. Creating filepath.... Input_file_path={file_path}\n")
    
    # Add the '-columnmappingv2' suffix to the file name
    colmap_file_path = file_path.split('.csv')[0]+'-columnmappingv2.csv'
    
    # Read input file
    input_df = read_file(file_path)
    logging.info(f"3. Reading input file path.... {input_df.shape}\n")
    
    # Starting basic preprocessing
    logging.info("4. Starting basic_preprocessing function....")
    input_df = basic_preprocessing(input_df)
    logging.info(f"5. basic_preprocessing function completed.... {input_df.shape}\n")
    
    # Starting create_columns_if_doesnt_exist function
    logging.info("6. Starting create_columns_if_doesnt_exist function....")
    input_df = create_columns_if_doesnt_exist(input_df)
    logging.info(f"7. create_columns_if_doesnt_exist function completed.... {input_df.shape}\n")
    
    # Save the DataFrame to a CSV file with the new name
    logging.info("8. Starting required_columns_exist function....")
    input_df = required_columns_exist(input_df, industry_profile, feature_columns, module_type)
    logging.info(f"9. required_columns_exist function completed.... {input_df.shape}\n")

    # Running clean_email function
    logging.info("10. Running clean_email function....  \n")
    if "email_address" in input_df.columns:
        input_df = clean_email(input_df)
    logging.info(f"11. clean_email function completed.... {input_df.shape} \n")
    
    # Running clean_name function
    logging.info(f"12. Running clean_name function....  \n")
    input_df = clean_name(input_df)
    logging.info(f"13. clean_name function completed.... {input_df.shape} \n")
    
    # Running clean_zipcode function
    logging.info(f"14. Running clean_zipcode function....  \n")
    input_df['zipcode'] = input_df.apply(lambda row: handle_zipcode(row['zipcode']), axis=1)
    logging.info(f"15. clean_zipcode function completed.... {input_df.shape} \n")

    # Selecting data for model creation according to zipcode frequency counts
    logging.info("16. Selecting data for model creation according to zipcode frequency counts....  \n")


    # Save the processed DataFrame to a CSV file
    input_df.to_csv(colmap_file_path, index=False)

    logging.info({"success":"True", "OriginalFilePath": file_path, "NewFilePath": colmap_file_path})
        
except Exception as e:
    exc = str(e).replace('"','').replace("'",'')
    logging.error({"success":"False", "OriginalFilePath": file_path, "NewFilePath": colmap_file_path, "error": "column_mapping_failure", "error_details": exc})




