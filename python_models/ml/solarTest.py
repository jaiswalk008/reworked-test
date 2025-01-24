import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

data = pd.read_csv(r"data/solarData.csv", low_memory=False)

features = [
    "solarPotential.maxArrayPanelsCount",
    "solarPotential.maxArrayAreaMeters2",
    "solarPotential.maxSunshineHoursPerYear",
    "solarPotential.wholeRoofStats.areaMeters2",
    "solarPotential.roofSegmentStats",
    "label",
]

X = data[features[:-1]]
y = data["label"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.01, random_state=41
)

model = DecisionTreeClassifier(
    criterion="gini", max_depth=10, splitter="best", random_state=41
)

model.fit(X_train, y_train)

preds = model.predict(X_test)

print(classification_report(y_test, preds))
