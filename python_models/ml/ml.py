import pandas as pd
import warnings
from ml.demo import GENERATE_BETTY_DEMOGRAPHIC_SCORE
from ml.financial import GENERATE_BETTY_FINANCIAL_SCORE
from ml.solar import GENERATE_BETTY_SOLAR_SCORE

warnings.filterwarnings("ignore", category=FutureWarning)


def is_apartment(address):
    """
    Checks if an address indicates an apartment based on keywords.

    Parameters:
    - address (str): Address to check.

    Returns:
    - bool: True if the address indicates an apartment, False otherwise.
    """
    keywords = ["apt", "apartment", "#", "unit"]
    for keyword in keywords:
        if keyword.lower() in address.lower():
            return True
    return False


def GENERATE_BETTY_SCORE(testFilepath: str, trainFilepath: str) -> None:
    """
    Generates BETTY score for test data and saves the results to a CSV file.

    Parameters:
    - testFilepath (str): File path to the test data CSV file.
    - trainFilepath (str): File path to the train data CSV file.
    """
    train = pd.read_csv(trainFilepath, low_memory=False, on_bad_lines="warn")
    test = pd.read_csv(testFilepath, low_memory=False, on_bad_lines="warn")

    test["is_apartment"] = test.apply(
        lambda row: is_apartment(str(row["BETTY_UPDATED_ADDRESS_LINE1"])), axis=1
    )

    test = GENERATE_BETTY_DEMOGRAPHIC_SCORE(test=test, train=train)
    test = GENERATE_BETTY_FINANCIAL_SCORE(test=test, train=train)
    test = GENERATE_BETTY_SOLAR_SCORE(test=test, train=train)

    test.loc[test["is_apartment"] == True, "BETTY_DEMOGRAPHIC_SCORE"] = 0
    test.loc[test["is_apartment"] == True, "BETTY_FINANCIAL_SCORE"] = 0
    test.loc[test["is_apartment"] == True, "BETTY_SOLAR_SCORE"] = 0

    test["BETTY_SCORE"] = (
        (0.5 * test["BETTY_DEMOGRAPHIC_SCORE"])
        + (0.4 * test["BETTY_FINANCIAL_SCORE"])
        + (0.1 * test["BETTY_SOLAR_SCORE"])
    )

    test.to_csv(testFilepath.replace(".csv", "") + "_BETTY_SCORE_ML.csv", index=False)
