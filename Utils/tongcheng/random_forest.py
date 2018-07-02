from sklearn import ensemble
from do_evaluation import evaluate_result


def train_rf(train_set, cols):
    x = train_set[cols]
    y = train_set["label"]
    clf = ensemble.RandomForestClassifier(n_estimators=100, criterion='gini', max_depth=3, max_features='auto', class_weight='balanced')
    clf.fit(x, y)
    return clf


def test_rf(clf, test_set, cols):
    x = test_set[cols]
    y = test_set["label"]
    proba = clf.predict_proba(x)[:, 1]
    alpha_list, prec, rec, fscore, tpr, fpr = evaluate_result(proba, y.values)
    return alpha_list, prec, rec, fscore, tpr, fpr


def do_rf(train_set, test_set, cols):
    clf = train_rf(train_set, cols)
    alpha_list, prec, rec, fscore, tpr, fpr = test_rf(clf, test_set, cols)
    return alpha_list, prec, rec, fscore, tpr, fpr