import pandas as pd
import os
from tqdm import tqdm
import itertools
import warnings

warnings.filterwarnings("ignore")


def getPosteriorProbability(data: pd.DataFrame, instance: dict) -> float:
    """
    Calculate the posterior probability for a given instance using Bayesian inference.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data.
    instance (dict): The instance for which to calculate the posterior probability.

    Returns:
    float: The posterior probability as a percentage, rounded to 2 decimal places.
    """
    conditional = data.copy()

    for feature, value in instance.items():
        conditional = conditional[conditional[feature] == value]
    try:
        likelihood = len(conditional[conditional["label"] == 1]) / len(
            data[data["label"] == 1]
        )
        prior = len(data[data["label"] == 1]) / len(data)
        evidence = len(conditional) / len(data)
        posterior = (likelihood * prior) / evidence
        return round(posterior * 100, 2)
    except:
        return None


def CALCULATE_BETTY_DEMO_SCORE_V1(trainData: pd.DataFrame, row: pd.Series) -> tuple:
    """
    Calculate the BETTY demographic score using a prioritized list of features.

    Parameters:
    trainData (pd.DataFrame): The dataframe containing the training data.
    row (pd.Series): The row from the test data for which to calculate the score.

    Returns:
    tuple: Posterior probability and the list of features used.
    """
    priority = [
        "potential_age",
        "lengthofresidence",
        "demo_gender",
        "politicalparty",
        "maritalstatus",
        "householdincome",
        "demo_currently_lives_in_address",
        "education",
        "householdsize",
    ]

    row = row[priority]
    instance = row.to_dict()

    posterior = getPosteriorProbability(trainData, instance=instance)

    while posterior is None:
        popped = priority.pop()
        instance.pop(popped)
        posterior = getPosteriorProbability(trainData, instance=instance)

    return posterior, priority


def getEveryPossibleCombination(feature: list[str]) -> list[list[str]]:
    """
    Generate every possible combination of a list of features.

    Parameters:
    feature (list): The list of features.

    Returns:
    list: A list of all possible combinations of the features.
    """
    n = len(feature)
    combinations = []

    for i in range(1, n + 1):
        combinations.append(list(itertools.combinations(feature, i)))

    return combinations


def filterDict(dictionary: dict, keys: list) -> dict:
    """
    Filter a dictionary to include only specified keys.

    Parameters:
    dictionary (dict): The dictionary to filter.
    keys (list): The list of keys to include in the filtered dictionary.

    Returns:
    dict: The filtered dictionary.
    """
    res = dict()
    for key in keys:
        if key in dictionary:
            res[key] = dictionary[key]

    return res


def checkFeatureCombos(
    trainData: pd.DataFrame, combinations: list, instance: dict
) -> pd.DataFrame:
    """
    Check posterior probabilities for every combination of features.

    Parameters:
    trainData (pd.DataFrame): The dataframe containing the training data.
    combinations (list): A list of feature combinations.
    instance (dict): The instance for which to calculate posterior probabilities.

    Returns:
    pd.DataFrame: Dataframe containing features used and their corresponding posterior probabilities.
    """
    featuresUsed = []
    score = []
    for combination in combinations:
        combination = list(combination)
        row = filterDict(instance, combination)
        posterior = getPosteriorProbability(data=trainData, instance=row)

        featuresUsed.append(combination)
        score.append(posterior)

    res = pd.DataFrame()
    res["featuresUsed"] = featuresUsed
    res["score"] = score
    res = res.dropna()
    return res


def CALCULATE_BETTY_DEMO_SCORE_V2(trainData: pd.DataFrame, row: dict):
    """
    Calculate the BETTY demographic score using every possible combination of features.

    Parameters:
    trainData (pd.DataFrame): The dataframe containing the training data.
    row (dict): The row from the test data for which to calculate the score.

    Returns:
    tuple: Posterior probability and the list of features used.
    """
    features = [
        "potential_age",
        "lengthofresidence",
        "demo_gender",
        "politicalparty",
        "maritalstatus",
        "householdincome",
        "demo_currently_lives_in_address",
        "education",
        "householdsize",
    ]

    everyPossibleCombo = getEveryPossibleCombination(features)

    rows = []

    for combo in everyPossibleCombo:
        rows.append(
            checkFeatureCombos(trainData=trainData, combinations=combo, instance=row)
        )

    res = pd.concat(rows)
    res = res.sort_values(by=["score"], ascending=False).iloc[0]
    return res["score"], res["featuresUsed"]


def GENERATE_BETTY_DEMOGRAPHIC_SCORE(
    testFilePath: pd.DataFrame, trainFilePath: pd.DataFrame
):
    """
    Generate the BETTY demographic score for each row in the test data.

    This is the main function to call for generating the BETTY SCORE.

    Parameters:
    testFilePath (str): The file path to the test data CSV file.
    trainFilePath (str): The file path to the training data CSV file.

    Note:
    In line 197, `CALCULATE_BETTY_DEMO_SCORE_V1` is called, which is significantly faster than its V2 counterpart.
    This is because V2 exhausts all possible choices of feature selection via brute force,
    while V1 uses statistical methods to pick the best set of features.
    The user may choose one over the other depending on the time they have.
    """
    test = pd.read_csv(testFilePath, low_memory=False)
    train = pd.read_csv(trainFilePath, low_memory=False)
    bettyDemoScore = []
    featuresUsed = []
    n = len(test.index)

    for _, row in tqdm(test.iterrows(), total=n, desc="BETTY_DEMOGRAPHIC_SCORE"):
        posterior, features = CALCULATE_BETTY_DEMO_SCORE_V1(trainData=train, row=row)
        bettyDemoScore.append(posterior)
        featuresUsed.append(features)

    test["BETTY_DEMOGRAPHIC_SCORE"] = bettyDemoScore
    test["FEATURES_USED_FOR_SCORE"] = featuresUsed

    test.to_csv(
        os.path.join(
            os.path.dirname(testFilePath),
            os.path.basename(testFilePath).replace(".csv", "")
            + "_BETTY_DEMO_SCORE.csv",
        ),
        index=False,
    )
