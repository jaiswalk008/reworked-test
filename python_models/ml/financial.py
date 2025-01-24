import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from tqdm import tqdm


class FINANCIAL_POTENTIAL:
    """
    Class for analyzing financial potential based on provided data and generating BETTY scores.
    """

    def __init__(self, data: pd.DataFrame) -> None:
        """
        Initializes the FINANCIAL_POTENTIAL instance.

        Parameters:
        - data (pd.DataFrame): DataFrame containing financial data.
        """
        self.data = data
        self.features = [
            "remainingLifetimeUtilityBill",
            "federalIncentive",
            "solarPotential.financialAnalyses.financialDetails.remainingLifetimeUtilityBill",
            "solarPotential.financialAnalyses.financialDetails.federalIncentive",
            "solarPotential.financialAnalyses.financialDetails.costOfElectricityWithoutSolar",
            "solarPotential.financialAnalyses.financialDetails.solarPercentage",
            "solarPotential.financialAnalyses.financialDetails.percentageExportedToGrid",
            "solarPotential.financialAnalyses.leasingSavings.annualLeasingCost",
            "solarPotential.financialAnalyses.leasingSavings.savings.savingsYear1",
            "solarPotential.financialAnalyses.leasingSavings.savings.savingsYear20",
            "solarPotential.financialAnalyses.leasingSavings.savings.savingsLifetime",
            "solarPotential.financialAnalyses.cashPurchaseSavings.outOfPocketCost",
            "label",
        ]
        self.data = self.data[self.features]
        self.data = self.data.fillna(0)
        self.X = self.data[self.features[:-1]]
        self.y = self.data["label"]
        self.model = RandomForestClassifier(
            criterion="gini", max_depth=15, random_state=41
        )
        self.model.fit(self.X, self.y)

    def CALCULATE_BETTY_SCORE(self, instance: dict) -> float:
        """
        Calculates the BETTY score for a given instance.

        Parameters:
        - instance (dict): Dictionary representing a single instance of financial data.

        Returns:
        - float: BETTY score for the given instance.
        """
        instance = pd.DataFrame([instance])
        instance = instance[self.features[:-1]]
        return self.model.predict_proba(instance)[0][1] * 100


def GENERATE_BETTY_FINANCIAL_SCORE(
    test: pd.DataFrame, train: pd.DataFrame
) -> pd.DataFrame:
    """
    Generates BETTY financial scores for instances in the test DataFrame.

    Parameters:
    - test (pd.DataFrame): DataFrame containing test instances.
    - train (pd.DataFrame): DataFrame containing training instances.

    Returns:
    - pd.DataFrame: DataFrame with added "BETTY_FINANCIAL_SCORE" and "FINANCIAL_FEATURES_USED" columns.
    """
    n = len(test)
    financial = FINANCIAL_POTENTIAL(train)
    scores = []
    for _, row in tqdm(test.iterrows(), total=n, desc="BETTY_FINANCIAL_SCORE"):
        score = financial.CALCULATE_BETTY_SCORE(row.to_dict())
        scores.append(score)
    test["BETTY_FINANCIAL_SCORE"] = scores
    test["FINANCIAL_FEATURES_USED"] = np.round(
        test[financial.features[:-1]].notnull().sum(axis=1), 2
    )
    test["FINANCIAL_FEATURES_USED"] = np.round(
        test["FINANCIAL_FEATURES_USED"] / (len(financial.features) - 1), 2
    )
    return test
