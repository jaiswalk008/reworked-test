import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
from distutils.command.build_scripts import first_line_re
import sys
import os
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from imblearn.over_sampling import SMOTE


def gather_print_results(total_number_of_true_inquiries, PredictedDF, testDF, algorithm):
    print("######## ALGO ####### = ", algorithm)
    dfr = pd. DataFrame({'Predicted': PredictedDF})
    merged = pd.concat([dfr, testDF], axis=1)
    print("Total number of predicted inquiries = ", dfr[dfr['Predicted'] == True].shape[0])
    total_number_of_inquiries_predicted_correctly = merged.loc[(merged['Inquiry'] == 1) & (merged['Predicted'] == True)].shape[0]
    print("Total number of inquiries predicted correctly = ", total_number_of_inquiries_predicted_correctly)
    cm = confusion_matrix(testDF['Inquiry'] ,PredictedDF)
    print("CM Accuracy Positives, should be high = ", round(cm[1][1]/(cm[1][1]+cm[0][1])*100,2))
    print("CM Accuracy Negatives, should be 0 = ", round(cm[1][0]/(cm[1][0]+cm[0][0])*100,2))
    print("Model accuracy", round((cm[0][0] + cm[1][1])/(cm[0][0]+cm[1][0]+cm[1][1] + cm[0][1])*100,2))
    print("Percentage accuracy = ", round(100/total_number_of_true_inquiries*total_number_of_inquiries_predicted_correctly, 2))
    model_accuracy = round((2*(100/total_number_of_true_inquiries*total_number_of_inquiries_predicted_correctly)+(cm[0][0] + cm[1][1])/(cm[0][0]+cm[1][0]+cm[1][1] + cm[0][1])*100)/3,2)
    print("FINAL METRIC::: Weighted average accuracy =", model_accuracy)
    

def data_setup(df):
    if 'Inquiry' in df.columns:
        df.Inquiry = df.Inquiry.fillna(False)
        df.Inquiry = df.Inquiry.replace(2,False)
        df.Inquiry = df.Inquiry.replace(1,True)

    df['potential_age'].fillna(df['potential_age'].mode()[0], inplace = True)
    df['dist_btw_site_mail_zip'].fillna(9999, inplace=True)
    
    #for name in df.columns:
    #    lots_missing = sum(df[name].isna())/len(df[name])>.05
    #    only_one_value = len(df[name].unique())==1
    #    if lots_missing or only_one_value:
    #        df = df.drop(name,axis=1)
    # df['full_name_missing'] = df['owner_full_name'].isna()
    df['num_missing_in_row'] = df.isna().sum(axis=1)
    """
    price_cols = ["MARKET TOTAL VALUE",
              "MARKET LAND VALUE",
              "ASSESSED TOTAL VALUE",
              "APPRAISED LAND VALUE",
              "ASSESSED LAND VALUE",
              "county_assessed_value"]
    price_cols = []
    for name in price_cols:
        for i in range(len(df[name])):
          df.at[i,name] = float(df.at[i,name].replace("$","").replace(",",""))
        df[name] = df[name].astype("float")
    df['owner_1_type'] = df['owner_1_type'].astype("object")
    """
    le = LabelEncoder()
    
    #ASSESSED LAND VALUE, MARKET TOTAL VALUE'
    #delinquent_tax_year', 'ASSESSED TOTAL VALUE
    #OWNER STATUS', 'MARKET LAND VALUE', 'county_assessed_value', 'delinquent_tax_value'
    #owner_1_type, APPRAISED LAND VALUE', 'LOT ACREAGE.1

    """
     feature_columns = ['potential_age','ASSESSED LAND VALUE', 
        'BETTY SCORE', 'lot_acreage', 'lot_area_sqft', 
       'ASSESSMENT YEAR', 'market_price', 
       'delinquent_tax_year', 'ASSESSED TOTAL VALUE',
       'OWNER STATUS', 'MARKET LAND VALUE', 'county_assessed_value', 'delinquent_tax_value', 
       'is_business', 'MARKET TOTAL VALUE', 'property_tax', 'dist_btw_site_mail_zip', 
       'LAND USE', 'owner_1_type', 'APPRAISED LAND VALUE', 'LOT ACREAGE.1', 'full_name_missing', 'num_missing_in_row']
    """
    
    """feature_columns = ['potential_age',
        'BETTY SCORE', 'lot_acreage', 'lot_area_sqft', 
        'market_price', 
       'is_business', 'dist_btw_site_mail_zip', 
        'full_name_missing', 'num_missing_in_row']
     """
    feature_columns = ['potential_age',
        'BETTY SCORE',  
       'is_business', 'dist_btw_site_mail_zip', 
        'full_name_missing', 'num_missing_in_row']   
    """feature_columns = ['potential_age',
        'BETTY SCORE', 'lot_acreage', 'lot_area_sqft', 
       'is_business',  'dist_btw_site_mail_zip', 
       'full_name_missing', 'num_missing_in_row']   """
    if 'Inquiry' in df.columns:
        feature_columns.append('Inquiry')
    updated_df = df[feature_columns].copy()


    for name in updated_df.columns:
     if updated_df[name].dtype == "object":
         le.fit(updated_df[name].unique())
         updated_df[name] = le.transform(updated_df[name])
    return updated_df

to_predict_file_path = sys.argv[1]
datapath = "~/Documents/RE/ValidationFiles/"
all_files = ["tim42rwr_appended_betty.csv", "tim43rwr_appended_betty.csv","tim44rwr_appended_betty.csv", "seekingland_rwr_new_appended_betty.csv"]

li = []

for filename in all_files:
    df = pd.read_csv(datapath+filename, index_col=None, header=0)
    li.append(df)

datasets = pd.concat(li, axis=0, ignore_index=True)

train_dataset = data_setup(datasets)
sm = SMOTE(random_state = 2)
No = train_dataset.loc[(train_dataset['Inquiry'] == False) & (train_dataset['BETTY SCORE'] <=0)]
Yes = train_dataset.loc[(train_dataset['Inquiry'] == True) & (train_dataset['BETTY SCORE'] > 200)]
new_dataset = pd.concat([No, Yes], axis=0)
total_number_of_true_inquiries = new_dataset[new_dataset['Inquiry'] == True].shape[0]
print("Total number of true inquiries = ",total_number_of_true_inquiries )
x = new_dataset.drop("Inquiry",axis=1)
y = np.array(new_dataset["Inquiry"])

x_train, x_test, y_train, y_test = train_test_split(x,y,stratify=y)
X_train_res, y_train_res = sm.fit_resample(x_train, y_train.ravel())


#undersampling_size = 800

#x_train = pd.concat([x_train[y_train == 0][0:undersampling_size],x_train[y_train != 0]])
#y_train = np.hstack([y_train[y_train == 0][0:undersampling_size],y_train[y_train != 0]])

# Last change was to introduce smote, rather than running fit on x_train, y_train, running it on smote's samples

################## HistGradientBoostingClassifier Start ##############
#categorical_mask = list((new_dataset.drop("Inquiry",axis=1).dtypes == "object").to_numpy())
categorical_mask = list((train_dataset.drop("Inquiry",axis=1).dtypes == "object").to_numpy())
mod = HistGradientBoostingClassifier(categorical_features = categorical_mask,
                                     verbose=0,
                                     max_iter = 100)
mod.fit(X_train_res, y_train_res)
Inquiry = mod.predict(x_test)
gather_print_results(total_number_of_true_inquiries, Inquiry, pd.DataFrame(y_test, columns = ["Inquiry"]), "HistGradientBoostingClassifier")
################## HistGradientBoostingClassifier End ##############

################## KNN Start ##############
knn=KNeighborsClassifier(n_neighbors=5)
knn.fit(X_train_res, y_train_res)
Inquiry =knn.predict(x_test)
gather_print_results(total_number_of_true_inquiries, Inquiry, pd.DataFrame(y_test, columns = ["Inquiry"]), "KNN")
################## KNN End ##############

################### RandomForest Start ##########
threshold = 0.9
classifier=RandomForestClassifier()
classifier.fit(X_train_res, y_train_res)
Inquiry = classifier.predict(x_test)
gather_print_results(total_number_of_true_inquiries, Inquiry, pd.DataFrame(y_test, columns = ["Inquiry"]), "RandomForest")
################### RandomForest End ##########


################### Naive Bayes Start ##########
model = GaussianNB()
model.fit(X_train_res, y_train_res)
Inquiry = model.predict(x_test)
gather_print_results(total_number_of_true_inquiries, Inquiry, pd.DataFrame(y_test, columns = ["Inquiry"]), "Naive Bayes")
################### Naive Bayes End ##########


###### Trying to actually predict a file ########
#df_test = pd.read_csv("~/Downloads/tim42enquiriesonly_rwr_appended_betty.csv", low_memory=False,na_values = "")
df_test = pd.read_csv(to_predict_file_path, low_memory=False,na_values = "")
feature_test = data_setup(df_test)
to_predict=feature_test
if 'Inquiry' in feature_test.columns:
    to_predict = feature_test.drop(columns='Inquiry', axis=1)
    total_number_of_true_inquiries = feature_test[feature_test['Inquiry'] == True].shape[0]
Inquiry = mod.predict(to_predict)
Inquiry_Probability = mod.predict_proba(to_predict)
df_predicted = pd.DataFrame({'Predicted': Inquiry})
df_predicted_probability = pd.DataFrame(Inquiry_Probability, columns = ['probabilty_score_no_inquiry','probabilty_score_getting_inquiry'])
df_result = pd.concat([df_predicted, df_predicted_probability, to_predict], axis=1)
print("Predicted values = ", df_result['Predicted'].value_counts())
if 'Inquiry' in feature_test.columns:
    gather_print_results(total_number_of_true_inquiries, Inquiry, feature_test, "HistGradientBoostingClassifier")

Inquiry = (knn.predict_proba(to_predict)[:,1] >= 0.66).astype(bool)
df_predicted = pd.DataFrame({'Predicted': Inquiry})
print(df_predicted.value_counts())
df_result_hist = pd.concat([df_predicted, to_predict], axis=1)
if 'Inquiry' in feature_test.columns:
    gather_print_results(total_number_of_true_inquiries, Inquiry, feature_test, "KNN")

Inquiry = (classifier.predict_proba(to_predict)[:,1] >= 0.993).astype(bool)
df_predicted = pd.DataFrame({'Predicted': Inquiry})
print(df_predicted.value_counts())
df_result_hist = pd.concat([df_predicted, to_predict], axis=1)
to_write = pd.concat([df_test, df_predicted], axis = 1)
to_write.Predicted = to_write.Predicted.replace(True, "MAIL")
to_write.Predicted = to_write.Predicted.replace(False, "DO_NOT_MAIL")
output_file_name = "BETTY_"+os.path.basename(to_predict_file_path)
to_write.to_csv(output_file_name,index=False)
if 'Inquiry' in feature_test.columns:
    gather_print_results(total_number_of_true_inquiries, Inquiry, feature_test, "RandomForest")

# before smote: Inquiry = (model.predict_proba(to_predict)[:,1] >= 0.99999998).astype(bool)
# with smote: 
#Inquiry = (model.predict_proba(to_predict)[:,1] >= 0.9999999999).astype(bool)
Inquiry = (model.predict_proba(to_predict)[:,1] >= 0.99999999).astype(bool)
#Inquiry = (model.predict_proba(to_predict)[:,1] >= 0.999999).astype(bool)
#Inquiry = (model.predict_proba(to_predict)[:,1] >= 0.99).astype(bool)
Inquiry_Probability = model.predict_proba(to_predict)
df_predicted = pd.DataFrame({'Predicted': Inquiry})
print(df_predicted.value_counts())
df_predicted_probability = pd.DataFrame(Inquiry_Probability, columns = ['probabilty_score_no_inquiry','probabilty_score_getting_inquiry'])
#print("Inquiry probability unique: ", df_predicted_probability['probabilty_score_getting_inquiry'].unique())
df_result = pd.concat([df_predicted, df_predicted_probability, to_predict], axis=1)
print("Predicted values = ", df_result['Predicted'].value_counts())
to_write = pd.concat([df_test, df_predicted], axis = 1)
to_write.Predicted = to_write.Predicted.replace(True, "MAIL")
to_write.Predicted = to_write.Predicted.replace(False, "DO_NOT_MAIL")
"""
cols_to_drop = ['dist_btw_site_mail_zip', 
    'deliquent_tax_ratio', 'is_business', 'potential_age', 'demo_address_verification_failed', 
    'demo_currently_lives_in_address', 'BETTY MELISSA', 'last_sale_date',  'full_name_missing', 'num_missing_in_row']
for col in cols_to_drop:
    try:
        to_write.drop(col, axis=1, inplace = True)
    except:
        pass
"""
output_file_name = "BETTY_"+os.path.basename(to_predict_file_path)
#to_write.to_csv(output_file_name,index=False)
if 'Inquiry' in feature_test.columns:  
    gather_print_results(total_number_of_true_inquiries, Inquiry, feature_test, "Naive Bayes")

