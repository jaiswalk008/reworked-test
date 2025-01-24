import pandas as pd
import re
import numpy as np
import math
import os
import json
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(message)s')

json_output = {
    'age': [25, 55],
    'gender': ['male', 'female'],
    'education': ['high school', 'bachelors', 'masters'],
    'income': ['0-25k', '25k-50k', '50k-75k', '75k-100k', '100k+']
}

def generate_criteria(individual_df):
    """
    This function generates criteria for data extraction based on the input dataframe.
    """
    if 'age' in individual_df.columns and individual_df['age'].nunique():
        json_output['age'] = [individual_df['age'].quantile(0.25), individual_df['age'].quantile(0.75)]
    if 'gender' in individual_df.columns and individual_df['gender'].nunique():
        json_output['gender'] = individual_df['gender'].value_counts().head(2).index.tolist()
    if 'education' in individual_df.columns and individual_df['education'].nunique():
        json_output['education'] = individual_df['education'].value_counts().head(3).index.tolist()
    if 'income' in individual_df.columns and individual_df['income'].nunique():
        json_output['income'] = individual_df['income'].value_counts().head(5).index.tolist()

    logging.info("=========================data profiling======================================= \n")
    profile_output = []

    for column in individual_df.columns:
        logging.info(f"Column: {column}")
        counts = individual_df[column].value_counts(dropna=False)
        percentages = individual_df[column].value_counts(normalize=True, dropna=False) * 100

        for value, count in counts.items():
            percentage = percentages[value]
            logging.info(f"Value: {value}, Count: {count}, Percentage: {percentage:.2f}%")
            profile_output.append(["Value: {value}, Count: {count}, Percentage: {percentage:.2f}%"])
        logging.info("-------------")

    json_output['profile_output'] = profile_output
    return json_output


try:
    logging.info("1. criteria_generationv2 code started.... \n")
    population_criteria_json = {}
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Specify a CSV filename ending with -preprocessv2, e.g., "filename-preprocessv2.csv"')
    args = parser.parse_args()
    file_path = args.file_path
    logging.info(f"2. reading preprocessv2 output file.... FILE - {file_path} \n")

    individual_df = pd.read_csv(file_path)
    logging.info(f"3. selecting  specific columns. \n")
    individual_df = individual_df[['gender', 'education', 'householdincome', 'age']]
    logging.info(f"4. generating criteria for data extraction.... \n")
    population_criteria_json = generate_criteria(individual_df)
    logging.info(f"5. generating criteria function completed.... \n")
    logging.info(f"6. data extraction criteria - {population_criteria_json} \n")
    criteria_json_file_path = file_path.split('.csv')[0]+'-criteriagenerationv2.json'
    
    with open(os.path.join(criteria_json_file_path), "w") as outfile:
        json.dump(population_criteria_json, outfile, indent=4)
        
    if 'profile_output' in population_criteria_json: del population_criteria_json['profile_output']
    
    logging.info({"success": "True", "OriginalFilePath": file_path, "criteria": population_criteria_json})
        
except Exception as e:
    exc = str(e).replace('"','').replace("'",'')
    logging.error({"success": "False", "OriginalFilePath": file_path, "error": "criteria_generation_failure", "error_details": exc, "criteria": population_criteria_json})