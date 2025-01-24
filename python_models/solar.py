import os
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
from tqdm import tqdm


class SOLAR_POTENTIAL:
    def __init__(self, data: pd.DataFrame) -> None:
        """
        Initialize the SOLAR_POTENTIAL class with training data and fit a KNeighborsClassifier.

        Parameters:
        data (pd.DataFrame): The dataframe containing the training data.
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
        self.data = self.data.fillna(value=-1)

        self.X = self.data[self.features[:-1]]
        self.y = self.data["label"]
        self.knn = KNeighborsClassifier(n_neighbors=5)
        self.knn.fit(self.X, self.y)

    def CALCULATE_BETTY_SCORE(self, instance: dict) -> float:
        """
        Calculate the BETTY solar score for a given instance using the trained KNeighborsClassifier.

        Parameters:
        instance (dict): The instance for which to calculate the solar score.

        Returns:
        float: The BETTY solar score as a percentage.
        """
        instance = pd.DataFrame([instance])
        instance = instance[self.features[:-1]]
        instance = instance.fillna(value=-1)
        return self.knn.predict_proba(instance)[0][1] * 100


def GENERATE_BETTY_SOLAR_SCORE(testFilePath: str, trainFilePath: str) -> None:
    """
    Generate the BETTY solar score for each row in the test data using the trained KNeighborsClassifier.

    This is the main function to call for generating the BETTY SOLAR SCORE.
    It automatically learns from the data present in the trainFilePath and generates the score for the data in testFilePath based on what it has learned.

    Parameters:
    testFilePath (str): The file path to the test data CSV file.
    trainFilePath (str): The file path to the training data CSV file.
    """
    trainData = pd.read_csv(trainFilePath, low_memory=False)
    testData = pd.read_csv(testFilePath, low_memory=False)

    n = len(testData)

    solar = SOLAR_POTENTIAL(trainData)

    scores = []

    for _, row in tqdm(testData.iterrows(), total=n, desc="BETTY_SOLAR_SCORE"):
        score = solar.CALCULATE_BETTY_SCORE(row.to_dict())
        scores.append(score)

    testData["BETTY_SOLAR_SCORE"] = scores

    testData.to_csv(
        os.path.join(
            os.path.dirname(testFilePath),
            os.path.basename(testFilePath).replace(".csv", "")
            + "_BETTY_SOLAR_SCORE.csv",
        ),
        index=False,
    )
