import pickle

from sklearn import svm
from sklearn.model_selection import train_test_split
from dfp_model.prec_rec import prec_and_recall


def doSVM(X, y, cols):
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.6)
    print "penalty:1.0"
    clf = train_svm(X_train[cols], y_train, 1.0, 'linear')
    test_pred = test_svm(X_test[cols], y_test, clf)
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall
    print "penalty:1.0"
    clf = train_svm(X_train[cols], y_train, 1.0)
    test_pred = test_svm(X_test[cols], y_test, clf)
    X_test["pred"] = test_pred
    pickle.dump(X_test, open("svm_pred", "wb"))
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall
    print "penalty:10.0"
    clf = train_svm(X_train[cols], y_train, 10.0)
    test_pred = test_svm(X_test[cols], y_test, clf)
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall
    print "penalty:100.0"
    clf = train_svm(X_train[cols], y_train, 100.0)
    test_pred = test_svm(X_test[cols], y_test, clf)
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall
    print "penalty:1000.0"
    clf = train_svm(X_train[cols], y_train, 1000.0)
    test_pred = test_svm(X_test[cols], y_test, clf)
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall
    print "penalty:10000.0"
    clf = train_svm(X_train[cols], y_train, 10000.0)
    test_pred = test_svm(X_test[cols], y_test, clf)
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall
    print "penalty:100000.0"
    clf = train_svm(X_train[cols], y_train, 100000.0)
    test_pred = test_svm(X_test[cols], y_test, clf)
    precision, recall, _, _ = prec_and_recall(test_pred, y_test.values)
    print precision, recall


def test_svm(x, y, clf):
    test_pred = clf.predict(x)
    return test_pred


def train_svm(x, y, penalty, kern='rbf'):
    clf = svm.SVC(C=penalty, tol=1e-6, max_iter=1000, kernel=kern)
    clf.fit(x, y)
    return clf