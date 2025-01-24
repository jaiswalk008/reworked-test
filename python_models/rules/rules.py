import pandas as pd

import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

import numpy as np
from rules.utils import is_apartment
from rules.solar import GENERATE_BETTY_SOLAR_SCORE
from rules.financial import GENERATE_BETTY_FINANCIAL_SCORE
from rules.demo import GENERATE_BETTY_DEMOGRAPHIC_SCORE


def GENERATE_BETTY_SCORE(testFilepath: str, trainFilepath: str) -> None:
    """
    Generates BETTY scores for the given test data file using rules-based methods.

    Parameters:
    - testFilepath (str): Filepath to the test data CSV file.
    - trainFilepath (str): Filepath to the train data CSV file.

    Returns:
    - None
    """
    train = pd.read_csv(trainFilepath, low_memory=False, on_bad_lines="warn")
    train = train[train["label"] == 1]

    test = pd.read_csv(testFilepath, low_memory=False, on_bad_lines="warn")
    test["is_apartment"] = test.apply(
        lambda row: is_apartment(str(row["BETTY_UPDATED_ADDRESS_LINE1"])), axis=1
    )

    print("Generating BETTY_SOLAR_SCORE...")
    test = GENERATE_BETTY_SOLAR_SCORE(test=test, train=train)

    print("Generating BETTY_FINANCIAL_SCORE...")
    test = GENERATE_BETTY_FINANCIAL_SCORE(test=test, train=train)

    print("Generating BETTY_DEMOGRAPHIC_SCORE...")
    test = GENERATE_BETTY_DEMOGRAPHIC_SCORE(data=test)
    test["BETTY_DEMOGRAPHIC_SCORE"] = np.round(test["BETTY_DEMOGRAPHIC_SCORE"] * 10)

    print("Generating BETTY_SCORE...")
    test["BETTY_SCORE"] = (
        (0.5 * test["BETTY_DEMOGRAPHIC_SCORE"])
        + (0.4 * test["BETTY_FINANCIAL_SCORE"])
        + (0.1 * test["BETTY_SOLAR_SCORE"])
    )

    test.loc[test["is_apartment"], "BETTY_SCORE"] = 0

    test.to_csv(
        testFilepath.replace(".csv", "") + "_BETTY_SCORE_rules_based.csv", index=False
    )
