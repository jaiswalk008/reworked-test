import os
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from tqdm import tqdm


class FINANCIAL_POTENTIAL:
    def __init__(self, data: pd.DataFrame) -> None:
        """
        Initialize the FINANCIAL_POTENTIAL class with training data and fit a RandomForestClassifier.

        Parameters:
        data (pd.DataFrame): The dataframe containing the training data.
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
        self.X = self.data[self.features[:-1]]
        self.y = self.data["label"]
        self.rfc = RandomForestClassifier(criterion="gini", max_depth=10)
        self.rfc.fit(self.X, self.y)

    def CALCULATE_BETTY_SCORE(self, instance: dict) -> float:
        """
        Calculate the BETTY financial score for a given instance using the trained RandomForestClassifier.

        Parameters:
        instance (dict): The instance for which to calculate the financial score.

        Returns:
        float: The BETTY financial score as a percentage.
        """
        instance = pd.DataFrame([instance])
        instance = instance[self.features[:-1]]
        return self.rfc.predict_proba(instance)[0][1] * 100


def GENERATE_BETTY_FINANCIAL_SCORE(testFilePath: str, trainFilePath: str) -> None:
    """
    Generate the BETTY financial score for each row in the test data using the trained RandomForestClassifier.

    This is the main function to call for generating the BETTY FINANCIAL SCORE.
    It automatically learns from the data present in the trainFilePath and generates the score for the data in testFilePath based on what it has learned.

    Parameters:
    testFilePath (str): The file path to the test data CSV file.
    trainFilePath (str): The file path to the training data CSV file.
    """
    trainData = pd.read_csv(trainFilePath, low_memory=False)
    testData = pd.read_csv(testFilePath, low_memory=False)

    n = len(testData)

    financial = FINANCIAL_POTENTIAL(trainData)

    scores = []

    for _, row in tqdm(testData.iterrows(), total=n, desc="BETTY_FINANCIAL_SCORE"):
        score = financial.CALCULATE_BETTY_SCORE(row.to_dict())
        scores.append(score)

    testData["BETTY_FINANCIAL_SCORE"] = scores

    testData.to_csv(
        os.path.join(
            os.path.dirname(testFilePath),
            os.path.basename(testFilePath).replace(".csv", "")
            + "_BETTY_FINANCIAL_SCORE.csv",
        ),
        index=False,
    )
