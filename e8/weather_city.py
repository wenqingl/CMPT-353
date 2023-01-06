import numpy as np
import pandas as pd
import sys
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

labelled = pd.read_csv(sys.argv[1])
unlabelled = pd.read_csv(sys.argv[2])

X = labelled.iloc[:,1:].values # include year?
y = labelled['city'].values
#print(X)

X_train, X_valid, y_train, y_valid = train_test_split(X, y)

bayes_model = make_pipeline(
    StandardScaler(),
    GaussianNB()
)
bayes_model.fit(X_train, y_train)
#print(bayes_model.score(X_valid, y_valid))


knn_model = make_pipeline(
    StandardScaler(),
    KNeighborsClassifier(n_neighbors = 5)
)
knn_model.fit(X_train, y_train)
#print(knn_model.score(X_valid, y_valid))


rf_model = make_pipeline(
    StandardScaler(),
    RandomForestClassifier(n_estimators=100, max_depth=10, min_samples_leaf=3)
)
rf_model.fit(X_train, y_train)
print("random forest:", rf_model.score(X_valid, y_valid))


X_predict = unlabelled.iloc[:,1:].values
y_predict = rf_model.predict(X_predict)
#print(y_predict)

pd.Series(y_predict).to_csv(sys.argv[3], index=False, header=False)
