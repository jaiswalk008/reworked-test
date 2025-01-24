import numpy as np
import pandas as pd
from sklearn import svm
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import confusion_matrix
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE, SVMSMOTE
import pickle
import logging
import modules.data_processing as data_processing
import sys
import glob

np.random.seed(2012)
logging.basicConfig(level=logging.INFO, format="%(message)s")

def create_model(filename):
  
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))

    datapath = os.path.join(python_scripts_path, "training_files/")   
    all_files = os.listdir(datapath)
    # all_files = ["tim42rwr_appended_betty.csv", "tim43rwr_appended_betty.csv","tim44rwr_appended_betty.csv", "seekingland_rwr_new_appended_betty.csv"]

    li = []
    print("calling create model")

    for filename in all_files:
        df = pd.read_csv(datapath+filename, index_col=None, header=0, low_memory=False)
        li.append(df)
    datasets = pd.concat(li, axis=0, ignore_index=True)

    # sampling 
    train_dataset = data_processing.data_setup(datasets)
    sm = SVMSMOTE(random_state = 2012)
    No = train_dataset.loc[(train_dataset['Inquiry'] == False) & (train_dataset['BETTY SCORE'] <=0)]
    Yes = train_dataset.loc[(train_dataset['Inquiry'] == True) & (train_dataset['BETTY SCORE'] > 200)]
    new_dataset = pd.concat([No, Yes], axis=0)
    x = new_dataset.drop("Inquiry",axis=1)
    y = np.array(new_dataset["Inquiry"])
    x_train, x_test, y_train, y_test = train_test_split(x,y,stratify=y)
    X_train_res, y_train_res = sm.fit_resample(x_train, y_train.ravel())

    ################### SVM Start ##########
    classifier=svm.SVC(kernel="poly", probability=True)
    classifier.fit(X_train_res, y_train_res)
    Inquiry = classifier.predict(x_test)

    # Save the trained model to disk
    model_filename = os.path.join(python_scripts_path, "02_14_24.pkl")
    with open(model_filename, 'wb') as file:
        pickle.dump(classifier, file)

def combine_training_files(output_file_name):
    
    python_scripts_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(python_scripts_path, "training_files") 

    csv_files = glob.glob(data_path + "/*.csv")

    # Read each CSV file into DataFrame
    # This creates a list of dataframes
    df_list = (pd.read_csv(file, low_memory = False) for file in csv_files)

    # Concatenate all DataFrames
    df   = pd.concat(df_list, ignore_index=True)
    df.to_csv(output_file_name, index=False)


for i,arg in enumerate(sys.argv):
      if arg == '--create-model':
          model_file_name = sys.argv[i+1]
          create_model(model_file_name)
      if arg == "--combine-training-files":
          output_file_name = sys.argv[i+1]
          combine_training_files(output_file_name)

