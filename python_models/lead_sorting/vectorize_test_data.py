# python vectorize_test_data.py --train_file_path=train_reiprint.csv --test_file_path=test_reiprint.csv --train_vectors_path=train_reiprint_vectors.csv --feature_columns "dateofbirth" "education" "gender" "householdincome" "potential_age" "density" "age_median" "divorced" "family_dual_income" "income_household_median" "home_value" "education_some_college" "race_white"
import sys
sys.path.insert(1, 'python_models')

import pandas as pd
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from sklearn.metrics.pairwise import cosine_similarity
from utils import preprocess_data, read_file, basic_preprocessing, create_derived_columns, LEAD_SORTING_COLUMNS
from utils import  append_data, append_zipcode_data
from utils import clean_name, clean_zipcode, clean_email, clean_data, apply_pca
from models import isolation_forest, one_class_svm, calculate_distance_and_anomaly, split_and_generate, calculate_metrics
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def ohe_and_standardize_test_data(test_df, python_scripts_path, base_file_path):
    """
    Transform test dataframe using previously saved OHE and Standard Scaler.

    :param test_df: Test dataframe.
    :param preprocess_path: Directory path where OHE and scaler were saved.
    :return: Transformed dataframe.
    """
    
    # Load OHE and StandardScaler
    ohe_path = os.path.join(python_scripts_path, base_file_path + "_ohe.pkl")
    with open(ohe_path, "rb") as f:
        ohe = pickle.load(f)
    
    scaler_path = os.path.join(python_scripts_path, base_file_path + "_scaler.pkl")
    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)
    
    # Identify categorical columns
    cat_cols = test_df.select_dtypes(['object']).columns.tolist()
    if len(cat_cols):
        # Apply OHE on test data using loaded OHE
        logging.info(f"    * Categorical columns -> {cat_cols}\n")
        test_ohe = pd.DataFrame(ohe.transform(test_df[cat_cols]).toarray(), columns=ohe.get_feature_names_out(cat_cols))
        test_df = test_df.drop(columns=cat_cols).reset_index(drop=True)
        test_df = pd.concat([test_df, test_ohe], axis=1)
        logging.info(f"    * Loaded the OHE model, modelpath -> {ohe_path}. Test data has been one-hot encoded.\n")
    # Standardize test data using loaded scaler
    # TODO: Madhur to replace meidan with train meidan using pickle.
    test_df = test_df.fillna(test_df.median())
    test_standard = pd.DataFrame(scaler.transform(test_df), columns=test_df.columns)
    return test_standard


try:
    logging.info("* Starting test data vectorization code.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_file_path', type=str, required=True, help='Please add a train CSV filename, eg - train_filename.csv')
    parser.add_argument('--test_file_path', type=str, required=True, help='Please add a test CSV filename, eg - test_filename.csv')
    parser.add_argument('--train_vectors_path', type=str, required=True, help='Please add a train vectors csv  filesnames, eg - train_vectors.csv')
    parser.add_argument('--feature_columns', metavar='S', type=str, nargs='+', required=True,
                        help='add space separated "strings" that will be used as features, eg - col1 col2 col3')

    args = parser.parse_args()
    train_file_path = args.train_file_path 
    base_file_path = train_file_path.rsplit('.', 1)[0]
    test_file_path = args.test_file_path 
    output_file_path = test_file_path.rsplit('.', 1)[0] + '_processed_betty.csv'
    train_vectors_path = args.train_vectors_path
    feature_columns = args.feature_columns
    logging.info(f"1. Loading data from filepath.... test_file_path = {test_file_path}, feature_columns = {feature_columns} \n")
    
    test_df = read_file(test_file_path)
    logging.info(f"2. test file loaded, test_df.shape -> {test_df.shape} \n")
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))
    vectorized_train_df = pd.read_csv(train_vectors_path)
    exp_train, exp_test = split_and_generate(vectorized_train_df)
    logging.info(f" Creating experimental train and test datasets.... exp_train.shape -> {exp_train.shape}, exp_test.shape -> {exp_test.shape} \n")
    exp_test_vectors = exp_test.drop(columns=['is_generated'])
    exp_test = calculate_distance_and_anomaly(exp_test_vectors, exp_train, exp_test)
    best_similarity_algorithm = calculate_metrics(exp_test)
    logging.info(f"4. Reading vectorized_train_df.... {vectorized_train_df.shape}\n")
    logging.info(f"5. Standardizing test data... {test_df.shape}\n")
    formatted_test_df = test_df[LEAD_SORTING_COLUMNS]
    standardized_test_df = ohe_and_standardize_test_data(formatted_test_df, python_scripts_path, base_file_path)
    logging.info(f"6. Ran OHE and standardscaler on test data.... {formatted_test_df.shape}\n")
    vectorized_test_df = apply_pca(standardized_test_df, python_scripts_path, base_file_path, 'test')
    
    logging.info(f"* Test data vectorized.... {vectorized_test_df.shape}\n")
    logging.info(f"* Computing various distances and detecting outliers....\n")
    test_df_with_score = calculate_distance_and_anomaly(vectorized_test_df, vectorized_train_df, test_df, best_similarity_algorithm)
    logging.info(f"* Distance and Outliers have been calculated, dataframe shape -> {test_df_with_score.shape}, writing the output file now -> {output_file_path}....\n")
    test_df_with_score = test_df.combine_first(test_df_with_score)

    # test_df_with_score = pd.concat([test_df, test_df_with_score])
    test_df_with_score.to_csv(output_file_path, index=False)
    logging.info({"success":"True","OriginalFilePath": test_file_path,"NewFilePath": output_file_path})
       
except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    pass
    logging.error({"success":"False","OriginalFilePath": test_file_path, "error": "vectorize_test_data", "error_details": exc})
