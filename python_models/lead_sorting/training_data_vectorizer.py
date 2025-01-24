# python training_data_vectorizer.py --train_file_path=train_reiprint.csv --feature_columns "dateofbirth" "education" "gender" "householdincome" "potential_age"
#"dateofbirth" "education" "gender" "householdincome" "potential_age"
import sys
# sys.path.append('/Users/harshit/Desktop/projects/reworked/process-control/python_models/lead_generation')
sys.path.insert(1, 'python_models')

import pandas as pd
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from utils import preprocess_data, read_file, append_zipcode_data, apply_pca, create_derived_columns, LEAD_SORTING_COLUMNS
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')


def ohe_and_standardize_train_data(df, python_scripts_path, base_file_path):
    """
    One-hot encode categorical features and standardize the dataframe.
    Saves the encoders and standard scaler.

    :param df: Input dataframe.
    :return: Modified dataframe.
    """
    # Identify categorical columns
    cat_cols = df.select_dtypes(['object']).columns.tolist()

    # OHE on categorical columns
    if len(cat_cols):
        logging.info(f"    * Categorical columns -> {cat_cols}\n")
        ohe = OneHotEncoder(drop='first', handle_unknown='ignore')
        df_ohe = pd.DataFrame(ohe.fit_transform(df[cat_cols]).toarray(), columns=ohe.get_feature_names_out(cat_cols))
        df = df.drop(columns=cat_cols).reset_index(drop=True)
        df = pd.concat([df, df_ohe], axis=1)
        
        # Save the OHE and StandardScaler
        ohe_path = os.path.join(python_scripts_path, base_file_path + "_ohe.pkl")
        with open(ohe_path, "wb") as f:
            pickle.dump(ohe, f)     
        logging.info(f"    * Categorical data has been one-hot encoded. OHE pickle file has been saved here -> {ohe_path}\n")
    # filling null values with median values.
    df = df.fillna(df.median())
    # Standardize data
    scaler = StandardScaler()
    df_standard = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
    scaler_path = os.path.join(python_scripts_path, base_file_path + "_scaler.pkl")
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)
    logging.info(f"    * Data has been scaled. standardscaler pickle file has been saved here -> {scaler_path}\n")
    return df_standard


def vectorize_training_data(dataframe):
    # Convert all columns to string datatype
    for col in dataframe.columns:
        dataframe[col] = dataframe[col].astype(str)
    
    feature_columns = list(dataframe.columns)    
    
    # Create a tfidf vector out of it
    # First, we'll concatenate all the rows to create a single string representation for each row
    dataframe['combined_features'] = dataframe.apply(lambda row: ' '.join(row.values), axis=1)
    tfidf_vectorizer = TfidfVectorizer(token_pattern=r'[^\s+]')
    tfidf_matrix = tfidf_vectorizer.fit_transform(dataframe['combined_features'])
        
    # Return the vectorized dataframe and a list of feature columns
    vectorized_dataframe = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())
    vectorized_feature_columns = list(vectorized_dataframe.columns)
    
    return feature_columns, vectorized_dataframe, tfidf_vectorizer


try:
    logging.info("\n1. Starting column_mappingv2 code.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_file_path', '-f', type=str, required=True, help='Please add a train CSV filename, eg - train_filename.csv')

    # Use nargs='+' to gather one or more values for the 'strings' argument
    parser.add_argument('--feature_columns', metavar='S', type=str, nargs='+', required=False,
                        help='add space separated "strings" that will be used as features, eg - col1 col2 col3')
    args = parser.parse_args()

    train_file_path = args.train_file_path
    base_file_path = train_file_path.rsplit('.', 1)[0]
    feature_columns = args.feature_columns
    logging.info(f"2. Loading Data.... train_file_path = {train_file_path}, feature_columns = {feature_columns} \n")
    
    train_df = read_file(train_file_path)
    logging.info(f"3. training file loaded, train_df.shape -> {train_df.shape} \n")
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))
    train_df = train_df[LEAD_SORTING_COLUMNS]
    train_df = train_df.drop_duplicates()
    logging.info(f"4. Standardizing training data.... {train_df.shape}\n")
    train_df = ohe_and_standardize_train_data(train_df, python_scripts_path, base_file_path)
    logging.info(f"5. Ran OHE and standardscaler on training data.... {train_df.shape}, columns present -> {train_df.columns}, total null values -> {train_df.isnull().sum().sum()}\n")
    vectorized_train_df = apply_pca(train_df, python_scripts_path, base_file_path, 'train')
    logging.info(f"6. Converting dataframe -> {train_df.shape} to pca dataframe -> {vectorized_train_df.shape}\n")
    logging.info(f"7. Training data vectorized.... vectorized_train_df -> {vectorized_train_df.shape}\n")
        
    vectorized_train_df.to_csv(f'{base_file_path}_vectors.csv', index=False)
    logging.info({"success":"True","OriginalFilePath": train_file_path,"feature_columns": feature_columns, "NewFilePath": "training_vectors.pkl"})
       
except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    pass
    logging.error({"success":"False","OriginalFilePath": train_file_path, "error": "training_data_vectorizer", "error_details": exc})
