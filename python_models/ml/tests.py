import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency


def chi_square_test(data: pd.DataFrame, feature_col: str, label_col: str) -> float:
    """
    Perform the Chi-square test for independence between a feature column and a label column.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data.
    feature_col (str): The name of the feature column.
    label_col (str): The name of the label column.

    Returns:
    float: The p-value from the Chi-square test.
    """
    contingency_table = pd.crosstab(data[feature_col], data[label_col])
    _, p, _, _ = chi2_contingency(contingency_table)
    return p


def cramers_v(data: pd.DataFrame, feature_col: str, label_col: str) -> float:
    """
    Calculate Cramér's V statistic for association between two categorical variables.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data.
    feature_col (str): The name of the feature column.
    label_col (str): The name of the label column.

    Returns:
    float: Cramér's V statistic.
    """
    contingency_table = pd.crosstab(data[feature_col], data[label_col])
    chi2, _, _, _ = chi2_contingency(contingency_table)
    n = contingency_table.sum().sum()
    phi2 = chi2 / n
    r, k = contingency_table.shape
    phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
    rcorr = r - ((r - 1) ** 2) / (n - 1)
    kcorr = k - ((k - 1) ** 2) / (n - 1)
    return np.sqrt(phi2corr / min((kcorr - 1), (rcorr - 1)))


def correlation_ratio(data: pd.DataFrame, feature_col: str, label_col: str) -> float:
    """
    Calculate the correlation ratio (η) for categorical-continuous association.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data.
    feature_col (str): The name of the feature column.
    label_col (str): The name of the label column.

    Returns:
    float: Correlation ratio or None if computation fails.
    """
    try:
        categories = data[feature_col].unique()
        groups = [
            data[data[feature_col] == category][label_col] for category in categories
        ]
        label_counts = data[label_col].value_counts()
        N = len(data)
        T = sum(label_counts**2) / N
        S = sum([sum(group.value_counts() ** 2) / len(group) for group in groups])
        return np.sqrt((S - T) / (N - 1))
    except:
        return None


def phi_coefficient(data: pd.DataFrame, feature_col: str, label_col: str) -> float:
    """
    Calculate the Phi coefficient for the association between two binary variables.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data.
    feature_col (str): The name of the feature column.
    label_col (str): The name of the label column.

    Returns:
    float: Phi coefficient.
    """
    contingency_table = pd.crosstab(data[feature_col], data[label_col])
    chi2, _, _, _ = chi2_contingency(contingency_table)
    phi = np.sqrt(chi2 / len(data))
    return phi


def mutual_information(data: pd.DataFrame, feature_col: str, label_col: str) -> float:
    """
    Calculate mutual information between two categorical variables.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data.
    feature_col (str): The name of the feature column.
    label_col (str): The name of the label column.

    Returns:
    float: Mutual information value.
    """
    contingency_table = pd.crosstab(data[feature_col], data[label_col])
    contingency_table = contingency_table.values
    col_totals = contingency_table.sum(axis=0)
    row_totals = contingency_table.sum(axis=1)
    total = contingency_table.sum()
    expected = np.outer(row_totals, col_totals) / total
    observed = contingency_table / total
    mi = np.sum(observed * np.log(observed / expected + 1e-9))
    return mi


def checkTestResults(data: pd.DataFrame) -> pd.DataFrame:
    """
    Conduct Chi-square tests on all features in the dataframe against the label column and return sorted results.

    This is the main function to import and use for performing statistical tests on the features of a dataframe.

    Parameters:
    data (pd.DataFrame): The dataframe containing the data. Assumes 'label' column exists as the target.

    Returns:
    pd.DataFrame: Dataframe with features and their corresponding p-values, sorted by p-value.

    Note:
    To replace the type of test conducted, replace the function call on line 135 with the function of your choice,
    such as `cramers_v`, `correlation_ratio`, `phi_coefficient`, or `mutual_information`.
    """
    feature = []
    score = []

    for col in data.columns:
        if col != "label":
            feature.append(col)
            res = chi_square_test(data, feature_col=col, label_col="label")
            score.append(res)

    testResult = pd.DataFrame()
    testResult["feature"] = feature
    testResult["score"] = score

    testResult = testResult.sort_values(by=["score"], ascending=True).reset_index(
        drop=True
    )

    return testResult
