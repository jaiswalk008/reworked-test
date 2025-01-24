import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import pickle

from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from scipy.spatial import distance
from sklearn.metrics import f1_score, recall_score, precision_score, confusion_matrix

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def generate_rows(data, n):
    # Calculate Q1, Q3 and IQR for each column
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    
    # Generate random rows
    rows = []
    for _ in range(n):
        random_row = np.random.uniform(Q1 - 3 * IQR, Q3 + 3 * IQR)
        rows.append(random_row)
    return pd.DataFrame(rows, columns=data.columns)

def split_and_generate(data):
    # Split data into train and test sets
    train, test = train_test_split(data, test_size=0.33, random_state=42, shuffle=True)
    
    # Generate random rows and append to test set
    generated_data = generate_rows(test, test.shape[0])
    
    # Add is_generated column
    test['is_generated'] = 0
    generated_data['is_generated'] = 1
    
    # Append generated data to test data
    test = pd.concat([test, generated_data], axis=0).reset_index(drop=True)
    test = test.sample(frac=1, random_state=42)
    return train, test

def lead_sort_betty_score(df , best_similarity_algorithm):
    if not best_similarity_algorithm:
        best_similarity_algorithm = 'minkowski_avg_similarity'
    # Scale values between 0 and 1
    df['betty_score'] = (df[best_similarity_algorithm] - df[best_similarity_algorithm].min()) / (df[best_similarity_algorithm].max() - df[best_similarity_algorithm].min())

    # Map values between -100 to 400
    df['betty_score'] = df['betty_score'] * 500 - 100

    # Adjust the median to 100
    median_diff = 100 - df['betty_score'].median()
    df['betty_score'] = df['betty_score'] + median_diff
    return df

def creating_clusters(vectorized_test, original_test):
    scaler = StandardScaler()
    vectorized_test_scaled = scaler.fit_transform(vectorized_test)
    n_clusters = min(5, vectorized_test_scaled.shape[0]-1)
    kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(vectorized_test_scaled)
    original_test['clusters'] = kmeans.labels_
    return original_test

def isolation_forest(train_data, test_data, contamination=0.05):
    # Create and fit the model
    iso_forest = IsolationForest(contamination=contamination, random_state=42)
    iso_forest.fit(train_data)
    # Predict anomalies in the test set
    anomalies = iso_forest.predict(test_data)
    return anomalies

def one_class_svm(train_data, test_data, nu=0.05, kernel='rbf'):
    # Create and fit the model
    one_class_svm = OneClassSVM(nu=nu, kernel=kernel, gamma='auto')
    one_class_svm.fit(train_data)
    # Predict anomalies in the test set
    anomalies = one_class_svm.predict(test_data)
    return anomalies

def rowwise_similarity(row1, row2, method='cosine'):
    """Compute row-wise similarity/distance using specified method."""
    if method == 'cosine':
        return 1 - distance.cosine(row1, row2)
    elif method == 'euclidean':
        return distance.euclidean(row1, row2)
    elif method == 'manhattan':
        return distance.cityblock(row1, row2)
    elif method == 'minkowski':
        return distance.minkowski(row1, row2)
    else:
        raise ValueError(f"Method '{method}' not recognized.")

def calculate_distance_and_anomaly(vectorized_test, vectorized_train, original_test, best_similarity_algorithm = None):
    """Compare each row in test to each row in train using specified method, and add min, max, avg columns."""
    original_test['isolation_forest'] = isolation_forest(vectorized_train, vectorized_test)
    original_test['one_class_svm'] = one_class_svm(vectorized_train, vectorized_test)
    if best_similarity_algorithm:
        methods = [best_similarity_algorithm.split('_')[0]]
    else:
        methods = ['cosine', 'euclidean', 'manhattan', 'minkowski']
    for method in methods:
        mins, maxs, avgs = [], [], []
        for index, test_row in vectorized_test.iterrows():
            similarities = [rowwise_similarity(test_row, train_row, method) for _, train_row in vectorized_train.iterrows()]

            mins.append(min(similarities))
            maxs.append(max(similarities))
            avgs.append(np.mean(similarities))
        
        original_test[f'{method}_min_similarity'] = mins
        original_test[f'{method}_max_similarity'] = maxs
        original_test[f'{method}_avg_similarity'] = avgs
        
    original_test = creating_clusters(vectorized_test, original_test)
    original_test = lead_sort_betty_score(original_test, best_similarity_algorithm)
    return original_test

def calculate_metrics(original_test):
    similarity_cols = [col for col in original_test.columns if 'similarity' in col]
    best_similarity_algorithm, best_accuracy = None, 0
    for col in similarity_cols:
        sorted_test = original_test.sort_values(by=col, ascending=True)
        top_half = sorted_test.head(sorted_test.shape[0] // 2)
        count_zeros_top_half = (top_half['is_generated'] == 0).sum()
        accuracy = round(1*count_zeros_top_half/top_half.shape[0], 3)
        logging.info(f"For Similarity score - {col}, Accuracy ==>  {count_zeros_top_half}//{top_half.shape[0]} = {accuracy}  \n")
        if accuracy > best_accuracy:
            best_similarity_algorithm, best_accuracy = col, accuracy
    logging.info(f"Best Similarity score - {best_similarity_algorithm}, Best Accuracy ==> {best_accuracy}\n")
    return best_similarity_algorithm

    
    



























