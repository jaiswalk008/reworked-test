import pandas as pd
from rules.utils import scoreBuilder


def GENERATE_BETTY_SOLAR_SCORE(test: pd.DataFrame, train: pd.DataFrame) -> pd.DataFrame:
    """
    Generates BETTY solar scores for the given test data based on the provided train data.

    Parameters:
    - test (pd.DataFrame): DataFrame containing test data.
    - train (pd.DataFrame): DataFrame containing train data.

    Returns:
    - pd.DataFrame: DataFrame with added "BETTY_SOLAR_SCORE" column.
    """
    features = [
        "solarPotential.maxArrayPanelsCount",
        "solarPotential.maxArrayAreaMeters2",
        "solarPotential.maxSunshineHoursPerYear",
        "solarPotential.wholeRoofStats.areaMeters2",
        "solarPotential.roofSegmentStats",
    ]

    scoreName = "BETTY_SOLAR_SCORE"
    test[scoreName] = 0

    for feature in features:
        test = scoreBuilder(
            test=test, train=train, feature=feature, scoreName=scoreName
        )

    test.loc[(test["is_apartment"] == True), scoreName] = 0

    return test
