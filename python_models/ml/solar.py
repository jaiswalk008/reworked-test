import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from tqdm import tqdm


class SOLAR_POTENTIAL:
    """
    Class for analyzing solar potential based on provided data and generating BETTY scores.
    """

    def __init__(self, data: pd.DataFrame) -> None:
        """
        Initializes the SOLAR_POTENTIAL instance.

        Parameters:
        - data (pd.DataFrame): DataFrame containing solar potential data.
        """
        self.data = data
        self.features = [
            "solarPotential.maxArrayPanelsCount",
            "solarPotential.maxArrayAreaMeters2",
            "solarPotential.maxSunshineHoursPerYear",
            "solarPotential.wholeRoofStats.areaMeters2",
            "solarPotential.roofSegmentStats",
            "label",
        ]
        self.data = self.data[self.features]
        self.X = self.data[self.features[:-1]]
        self.y = self.data["label"]
        self.model = DecisionTreeClassifier(
            criterion="gini", max_depth=10, splitter="best", random_state=41
        )
        self.model.fit(self.X, self.y)

    def CALCULATE_BETTY_SCORE(self, instance: dict) -> float:
        """
        Calculates the BETTY score for a given instance.

        Parameters:
        - instance (dict): Dictionary representing a single instance of solar potential data.

        Returns:
        - float: BETTY score for the given instance.
        """
        instance = pd.DataFrame([instance])
        instance = instance[self.features[:-1]]
        instance = instance.fillna(value=-1)
        return self.model.predict_proba(instance)[0][1] * 100


def GENERATE_BETTY_SOLAR_SCORE(test, train) -> pd.DataFrame:
    """
    Generates BETTY solar scores for instances in the test DataFrame.

    Parameters:
    - test (pd.DataFrame): DataFrame containing test instances.
    - train (pd.DataFrame): DataFrame containing training instances.

    Returns:
    - pd.DataFrame: DataFrame with added "BETTY_SOLAR_SCORE" and "SOLAR_FEATURES_USED" columns.
    """
    n = len(test)
    solar = SOLAR_POTENTIAL(train)
    scores = []
    for _, row in tqdm(test.iterrows(), total=n, desc="BETTY_SOLAR_SCORE"):
        score = solar.CALCULATE_BETTY_SCORE(row.to_dict())
        scores.append(score)
    test["BETTY_SOLAR_SCORE"] = scores
    test["SOLAR_FEATURES_USED"] = np.round(
        test[solar.features[:-1]].notnull().sum(axis=1), 2
    )
    test["SOLAR_FEATURES_USED"] = np.round(
        test["SOLAR_FEATURES_USED"] / (len(solar.features) - 1), 2
    )
    return test
