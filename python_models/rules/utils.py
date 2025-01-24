import pandas as pd


def is_apartment(address):
    keywords = ["apt", "apartment", "#", "unit"]
    for keyword in keywords:
        if keyword.lower() in address.lower():
            return True
    return False


def scoreBuilder(
    test: pd.DataFrame, train: pd.DataFrame, feature: str, scoreName: str
) -> pd.DataFrame:
    """
    Builds scores based on the given feature and updates the provided DataFrame with the scores.

    Parameters:
    - test (pd.DataFrame): DataFrame containing the test data.
    - train (pd.DataFrame): DataFrame containing the train data.
    - feature (str): Name of the feature for which scores are to be built.
    - scoreName (str): Name of the column to store the scores in the DataFrame.

    Returns:
    - pd.DataFrame: DataFrame with updated scores.
    """
    twentyFifth = test[feature].quantile(0.25)
    seventyFifth = train[feature].quantile(0.75)
    minimum = train[feature].min()
    maximum = train[feature].max()

    test.loc[
        (test[feature] >= twentyFifth) & (test[feature] <= seventyFifth), scoreName
    ] = (test[scoreName] + 30)
    test.loc[(test[feature] > minimum) & (test[feature] < twentyFifth), scoreName] = (
        test[scoreName] + 20
    )
    test.loc[(test[feature] > seventyFifth) & (test[feature] <= maximum), scoreName] = (
        test[scoreName] + 20
    )
    test.loc[(test[feature] > maximum), scoreName] = test[scoreName] + 10

    return test
