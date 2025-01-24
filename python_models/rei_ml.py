import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
import pickle
import modules.data_processing as data_processing
np.random.seed(2012)

class rei_ml:

    global gather_print_results
    def gather_print_results(total_number_of_true_inquiries, PredictedDF, testDF, algorithm):
        print("######## ALGO ####### = ", algorithm)
        dfr = pd. DataFrame({'Predicted': PredictedDF})
        merged = pd.concat([dfr, testDF], axis=1)
        print("Total number of total_number_of_true_inquiries = ", total_number_of_true_inquiries)
        totalPredictedTrueEnquired = dfr[dfr['Predicted'] == True].shape[0]
        totalPredictedFalseEnquired = dfr[dfr['Predicted'] == False].shape[0]
        totalEnquires = totalPredictedTrueEnquired + totalPredictedFalseEnquired
        print("Total number of predicted inquiries = ", totalPredictedTrueEnquired)
        print("Total number of predicted false inquiries = ", totalPredictedFalseEnquired)
        total_number_of_inquiries_predicted_correctly = merged.loc[(merged['Inquiry'] == 1) & (merged['Predicted'] == True)].shape[0]
        print("Total number of inquiries predicted correctly = ", total_number_of_inquiries_predicted_correctly)
        predictedPercentage = 100 - ((total_number_of_true_inquiries - total_number_of_inquiries_predicted_correctly)/total_number_of_true_inquiries ) *100
        percentageEleminated = ((totalPredictedFalseEnquired)/totalEnquires ) *100
        print('predictedPercentage',predictedPercentage)
        print('percentageEleminated',percentageEleminated)
        cm = confusion_matrix(testDF['Inquiry'] ,PredictedDF)
        print('confusion matrix', cm)
        print("CM Accuracy Positives, should be high = ", round(cm[1][1]/(cm[1][1]+cm[0][1])*100,2))
        print("CM Accuracy Negatives, should be 0 = ", round(cm[1][0]/(cm[1][0]+cm[0][0])*100,2))
        print("Model accuracy", round((cm[0][0] + cm[1][1])/(cm[0][0]+cm[1][0]+cm[1][1] + cm[0][1])*100,2))
        print("Percentage accuracy = ", round(100/total_number_of_true_inquiries*total_number_of_inquiries_predicted_correctly, 2))
        model_accuracy = round((2*(100/total_number_of_true_inquiries*total_number_of_inquiries_predicted_correctly)+(cm[0][0] + cm[1][1])/(cm[0][0]+cm[1][0]+cm[1][1] + cm[0][1])*100)/3,2)
        print("FINAL METRIC::: Weighted average accuracy =", model_accuracy)
        return predictedPercentage,percentageEleminated
        
    global process_ml
    def process_ml(df_test):
        
        python_scripts_path = os.path.dirname(os.path.abspath(__file__))
        ################### RandomForest End ##########
        model_filename = os.path.join(python_scripts_path, "02_14_24.pkl")
        # Load the trained model from disk
        with open(model_filename, 'rb') as file:
            classifier = pickle.load(file)

        ###### Trying to actually predict a file ########
        feature_test = data_processing.data_setup(df_test)
        if 'Inquiry' in feature_test.columns:
            feature_test = feature_test.drop('Inquiry', axis=1)
        to_predict=feature_test

        Inquiry = (classifier.predict_proba(to_predict)[:,1] >= 0.693).astype(bool)
        # total_number_of_true_inquiries = df_test[df_test['Inquiry'] == True].shape[0]
        # print('Inquiry',np.unique(Inquiry))
        # model_accuracy = gather_print_results(total_number_of_true_inquiries, Inquiry, pd.DataFrame(df_test, columns = ["Inquiry"]), "RandomForest")
        
        df_predicted = pd.DataFrame({'Predicted': Inquiry})
        to_write = pd.concat([df_test, df_predicted], axis = 1)
        to_write.Predicted = to_write.Predicted.replace(True, "CONTACT")
        to_write.Predicted = to_write.Predicted.replace(False, "DO_NOT_CONTACT")
        return to_write

