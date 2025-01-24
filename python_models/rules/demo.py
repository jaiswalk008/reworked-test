import pandas as pd


def GENERATE_BETTY_DEMOGRAPHIC_SCORE(data: pd.DataFrame) -> pd.DataFrame:
    """
    Generates BETTY demographic scores for the given data.

    Parameters:
    - data (pd.DataFrame): DataFrame containing demographic data.

    Returns:
    - pd.DataFrame: DataFrame with added "BETTY_DEMOGRAPHIC_SCORE" column.
    """
    data["BETTY_DEMOGRAPHIC_SCORE"] = 0

    if "date_of_death" in data.columns:
        data.loc[data["date_of_death"].notnull(), "BETTY_DEMOGRAPHIC_SCORE"] = (
            data["BETTY_DEMOGRAPHIC_SCORE"] - 195
        )

    ageColumn = "potential_age"
    if "age" in data.columns:
        ageColumn = "age"

    twentyFifth = data[ageColumn].quantile(0.25)
    seventyFifth = data[ageColumn].quantile(0.75)
    minimum = data[ageColumn].min()
    maximum = data[ageColumn].max()

    data.loc[data[ageColumn].isnull(), "BETTY_DEMOGRAPHIC_SCORE"] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )
    data.loc[
        (data[ageColumn] >= twentyFifth) & (data[ageColumn] <= seventyFifth),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 30
    )
    data.loc[
        (data[ageColumn] > seventyFifth) & (data[ageColumn] <= maximum),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 20
    )
    data.loc[
        (data[ageColumn] >= minimum) & (data[ageColumn] < twentyFifth),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )
    data.loc[(data[ageColumn] > maximum), "BETTY_DEMOGRAPHIC_SCORE"] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )

    data.loc[data["ownrent"] == "Definite Owner", "BETTY_DEMOGRAPHIC_SCORE"] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 30
    )

    data.loc[data["politicalparty"] == "Democrat", "BETTY_DEMOGRAPHIC_SCORE"] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )

    data.loc[
        data["maritalstatus"] == "Definitely Married", "BETTY_DEMOGRAPHIC_SCORE"
    ] = (data["BETTY_DEMOGRAPHIC_SCORE"] + 10)
    data.loc[
        data["maritalstatus"] == "Definitely Single", "BETTY_DEMOGRAPHIC_SCORE"
    ] = (data["BETTY_DEMOGRAPHIC_SCORE"] + 10)

    data.loc[
        data["demo_currently_lives_in_address"] == True, "BETTY_DEMOGRAPHIC_SCORE"
    ] = (data["BETTY_DEMOGRAPHIC_SCORE"] + 10)

    data.loc[
        ((data["demo_gender"] == "M") | (data["demo_gender"] == "unkownn")),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 20
    )

    data.loc[(data["education"] == "Graduate School"), "BETTY_DEMOGRAPHIC_SCORE"] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )

    data.loc[
        (data["presenceofchildren"] == "Children Present"), "BETTY_DEMOGRAPHIC_SCORE"
    ] = (data["BETTY_DEMOGRAPHIC_SCORE"] + 10)

    data.loc[
        (
            (data["lengthofresidence"] == "15+")
            | (data["lengthofresidence"] == "Less than 1 year")
        ),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] - 10
    )
    data.loc[
        (
            (data["lengthofresidence"] == "3-4")
            | (data["lengthofresidence"] == "4-5")
            | (data["lengthofresidence"] == "2-3")
            | (data["lengthofresidence"] == "5-6")
            | (data["lengthofresidence"] == "6-7")
        ),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 20
    )

    data.loc[
        (
            (data["lengthofresidence"] == "1-2")
            | (data["lengthofresidence"] == "7-8")
            | (data["lengthofresidence"] == "9-10")
        ),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )

    data.loc[
        ((data["householdsize"] == 2) | (data["householdsize"] == 3)),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 20
    )
    data.loc[
        ((data["householdsize"] == 1) | (data["householdsize"] == 5)),
        "BETTY_DEMOGRAPHIC_SCORE",
    ] = (
        data["BETTY_DEMOGRAPHIC_SCORE"] + 10
    )

    # Zero'ing out of score
    data.loc[data["ownrent"] == "Definite Renter", "BETTY_DEMOGRAPHIC_SCORE"] = 0

    if "is_business" in data.columns:
        data.loc[data["is_business"] == True, "BETTY_DEMOGRAPHIC_SCORE"] = 0

    if "is_public_entity" in data.columns:
        data.loc[data["is_public_entity"] == True, "BETTY_DEMOGRAPHIC_SCORE"] = 0

    data.loc[data["owner_full_name"].isnull(), "BETTY_DEMOGRAPHIC_SCORE"] = 0

    if "do_not_mail" in data.columns:
        data.loc[data["do_not_mail"] == True, "BETTY_DEMOGRAPHIC_SCORE"] = 0

    if "demo_address_verification_failed" in data.columns:
        data.loc[
            data["demo_address_verification_failed"] == True, "BETTY_DEMOGRAPHIC_SCORE"
        ] = 0

    if "do_not_contact" in data.columns:
        data.loc[data["do_not_contact"] == "DNC", "BETTY_DEMOGRAPHIC_SCORE"] = 0

    return data
