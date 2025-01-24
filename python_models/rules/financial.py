import pandas as pd
from rules.utils import scoreBuilder


def GENERATE_BETTY_FINANCIAL_SCORE(
    test: pd.DataFrame, train: pd.DataFrame
) -> pd.DataFrame:
    """
    Generates BETTY financial scores for the given test data based on the provided train data.

    Parameters:
    - test (pd.DataFrame): DataFrame containing test data.
    - train (pd.DataFrame): DataFrame containing train data.

    Returns:
    - pd.DataFrame: DataFrame with added "BETTY_FINANCIAL_SCORE" column.
    """
    features = [
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
    ]

    scoreName = "BETTY_FINANCIAL_SCORE"

    test[scoreName] = 0

    for feature in features:
        test = scoreBuilder(
            test=test, train=train, feature=feature, scoreName=scoreName
        )

    test.loc[(test["is_apartment"] == True), scoreName] = 0

    return test
