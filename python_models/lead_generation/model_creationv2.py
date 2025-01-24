"""
HOW TO RUN THIS FILE:
1. Place the input file in the 'data' folder with an extension of 'preprocessv2.csv'.
2. Navigate outside the 'data' folder and run this file using the following command:
    python .\modelcreationv2.py --file_path "file.xlsx"
3. The CSV output file will be generated inside the 'data' folder with a suffix of 'modelcreationv2.csv'.   
"""

import pandas as pd
import numpy as np
from dotenv import load_dotenv
load_dotenv()
import math
import os
import logging
import argparse
import json
import gc
import pickle
from datetime import datetime

# from sklearn.preprocessing import StandardScaler
# from scipy.spatial import distance_matrix
from sklearn.preprocessing import MinMaxScaler

min_max_scaler = MinMaxScaler(feature_range=(0.1, .95))

logging.basicConfig(level=logging.INFO, format='%(message)s')

def get_zipcode_density(individual_df):
    """
    This function calculates the traffic density, i.e., the number of sales/conversion/purchases per zipcode
    as compared to the total dataset. The output later helps in creating the traffic density of all 41k data using
    zipcode clustering data. 
    """
    individual_df.columns = ['_'.join(col.lower().split()) for col in individual_df.columns]
    individual_df.rename(columns={'postalcode': 'zipcode'}, inplace=True)
    individual_df['zipcode'].apply(lambda x: np.nan if pd.isna(x) else str(x).split('-')[0])
    individual_df.dropna(subset=['zipcode'], inplace=True)
    individual_df['zipcode'] = individual_df['zipcode'].astype('str')
    
    zipcode_density_df = individual_df.groupby('zipcode').size().reset_index(name='count')
    zipcode_density_df['count_density'] = zipcode_density_df['count']/zipcode_density_df['count'].sum()
    return zipcode_density_df

def calculating_probability_per_zipcode(zipcode_cluster_mapping_df, zipcode_density_df, distance_matrix_data, cluster_info):
        """
        This function calculates the probability of conversion for each zipcode using the formula.
        probability+= 1/Di * exp(count_density(i))
        i.e. for each row belonging to cluster C, it sums up the above formula where distance Di is the distance between that row
        to each row in the cluster D and multiplied by the count density of each row in the cluster.
        Zipcode with no count density information just contributes to the formula with 1/Di.
        For zipcode with 0 distance, the second nearest distance is taken.
        """
        distance_matrix_data.set_index('zip', inplace=True)
        probability_dict = {}
        for row in zipcode_cluster_mapping_df.itertuples():
            probability = 0
            target_cluster = row.cluster_label
            target_cluster_index = cluster_info[str(target_cluster)]
            zipcode_combinations = [(min(row.zip, idx), max(row.zip, idx)) for idx in target_cluster_index]

            distance_combinations = [distance_matrix_data[str(colval)][int(rowval)] for rowval, colval in  zipcode_combinations]
            non_zero_numbers = [num for num in distance_combinations if num != 0]
            second_min = sorted(non_zero_numbers)[0]
            
            for idx, dist in zip(zipcode_combinations, distance_combinations):
                rowval, colval = idx[0], idx[1] 
                if dist == 0:
                    dist = second_min
                if colval in zipcode_density_df.index:
                    probability += (1.0/dist) * math.exp(.1 * zipcode_density_df.iloc[colval]['count_density'])
                else:
                    probability+=(1.0/dist)
            
            probability_dict[row.zip]=probability
        zipcode_probability_df = pd.DataFrame(list(probability_dict.items()), columns=['zip', 'conversion_probability'])
        return zipcode_probability_df

try:
    logging.info("1. modelcreationv2 code started.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Please add a CSV filename ending with -preprocessv2, eg - filename-preprocessv2.csv')
    args = parser.parse_args()
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))
    parquet_files_path = os.getenv('PARQUET_FILES_PATH')
    
    file_path = args.file_path
    modelcreation_file_path = file_path.split('.csv')[0]+'-modelcreationv2.csv'
    logging.info(f"2. reading preprocess file.... FILE - {file_path} \n")

    individual_df = pd.read_csv(file_path)
    
    logging.info(f"3. creating zipcode density data.... \n")
    zipcode_density_df = get_zipcode_density(individual_df)
    
    logging.info(f"4. zipcode density data.... {zipcode_density_df.shape}\n")
    zipcode_cluster_mapping_df = pd.read_parquet(os.path.join(parquet_files_path, 'reduced_23k_processed_zipcode_data.parquet'))
    logging.info(f"5. reading zipcode cluster mapping data.... {zipcode_cluster_mapping_df.shape}\n")

    logging.info(f"6. reading zipcode clusters.... \n")
    with open(os.path.join(parquet_files_path, 'reduced_23k_cluster_zipcode_mapping.json'), 'r') as file:
        cluster_info = json.load(file)
    logging.info(f"7. zipcode clusters have been created.... \n")

    # distance_matrix_data.parquet was an 8gb file, so we are retiring it and using the reduced_f16_distance_matrix_data.pkl.gz file instead
    # the reduced_f16_distance_matrix_data.pkl.gz is a 1.3 gb file and we have changed the datatype from float64 to float16.
    # distance_matrix_data = pd.read_parquet(os.path.join(parquet_files_path, 'distance_matrix_data.parquet'))
    distance_matrix_data = pd.read_pickle(os.path.join(parquet_files_path, "reduced_f16_23k_distance_matrix_data.pkl.gz"))
    logging.info(f"8. reading distance matrix.... {distance_matrix_data.shape}\n")
    logging.info(f"9. calculating conversion probability per zipcode.... \n")
    zipcode_probaility_df = calculating_probability_per_zipcode(zipcode_cluster_mapping_df, zipcode_density_df, distance_matrix_data, cluster_info)
    del distance_matrix_data
    del zipcode_density_df
    gc.collect()
    zipcode_cluster_mapping_df = zipcode_cluster_mapping_df.merge(zipcode_probaility_df,on='zip', how='left')
    zipcode_cluster_mapping_df['conversion_probability'].fillna( zipcode_cluster_mapping_df['conversion_probability'].median(), inplace=True)
    zipcode_cluster_mapping_df.drop_duplicates(subset=['zip'], inplace=True)
    zipcode_cluster_mapping_df['conversion_probability'] = min_max_scaler.fit_transform(zipcode_cluster_mapping_df[['conversion_probability']])
    logging.info(f"10. conversion probability per zipcode has been calculated.... {zipcode_cluster_mapping_df.shape} \n")
    
    df1 = zipcode_cluster_mapping_df[['conversion_probability', 'zip']]
    df1_sorted = df1.sort_values(by='conversion_probability', ascending=False)

    logging.info(f"11. sorting and writing modelcreation output file.... \n")

    df1_sorted.to_csv(modelcreation_file_path, index=False)
    logging.info({"success":"True", "OriginalFilePath": file_path, "NewFilePath": modelcreation_file_path})
        
except Exception as e:
    exc = str(e).replace('"','').replace("'",'')
    logging.error({"success":"False", "OriginalFilePath": file_path, "NewFilePath": modelcreation_file_path, "error": "model_creation_failure", "error_details": exc})