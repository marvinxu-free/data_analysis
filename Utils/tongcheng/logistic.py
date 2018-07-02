import numpy as np
from sklearn import linear_model
from statsmodels import api as sm


def do_logistic(trainSet, testSet, cols, alpha):
    """
    This function is to train model and test model, also calculate the precision and recall.

    Parameter
    ---------
    x: the dataframe including the independent variables
    y: the dataframe of label
    cols: the specific columns used to train model
    alpha: the threshold of label
    Returns
    -------
    no return

    """
    # X_train, X_test, y_train, y_test = train_test_split(x, y, train_size=0.6)

    X_train = trainSet
    y_train = trainSet["label"].values
    X_test = testSet
    y_test = testSet["label"].values

    # X_train = x
    # y_train = y
    # do_stats_model(X_train[cols], y_train)
    clf = train_logistic(X_train, y_train, cols)
    print "coefficient of logistic model:\n"
    print [clf.intercept_, clf.coef_]
    test_prob, test_pred = test_logistic(X_test, cols, clf, alpha)
    X_test["prob_tmp"] = test_prob
    X_test["pred_tmp"] = test_pred
    if "prob" in X_test.columns:
        X_test["prob"] = X_test[["prob", "prob_tmp"]].max(axis=1)
        X_test["pred"] = X_test[["pred", "pred_tmp"]].max(axis=1)
    else:
        X_test["prob"] = X_test["prob_tmp"]
        X_test["pred"] = X_test["pred_tmp"]
    # pickle.dump(X_test, open(sys.argv[2] + "_score_dfp_mcid", "wb"))
    # print "Precision and Recall:\n"
    return do_online_prec_rec(X_test)
    # return prec_and_recall(X_test["pred"].values, y_test)


def train_logistic(x, y, cols):
    """
    This function is to train model.

    Parameter
    ---------
    x: the train set of dataframe including the independent variables
    y: the train set of dataframe of label

    Returns
    -------
    clf: the result of modeling

    """
    x = x[cols]
    lr = linear_model.LinearRegression(fit_intercept=True)
    lr.fit(x, y)
    clf = linear_model.LogisticRegression(penalty='l2', fit_intercept=True, solver='lbfgs', tol=1e-6, C=1.0,
                                          max_iter=1000, random_state=40,
                                          # class_weight='balanced'
                                          )
    clf.fit(x, y)
    return clf


def test_logistic(x, cols, clf, alpha=0.5):
    """
    This function is to test model.

    Parameter
    ---------
    x: the test set of dataframe including the independent variables
    y: the test set of dataframe of label
    clf: the training result of model

    Returns
    -------
    test_prob: the probability of label 1 of the test set
    test_pred: the predict label under specific alpha

    """
    global model_beta
    x = x[cols]
    new_cols = ["intercept"]
    new_cols.extend(cols)
    beta = model_beta[new_cols].values.T
    # print beta
    # if sys.argv[2] == 'ios':
    #     beta = [-8.5529, -1.59, 4.5875, 10.3380, 10.0, 9.8633]
    #     # beta = [-6.97870393, 1.04358171, 2.5911348, 2.33785585, 3.87863428, 2.40296212, 6.10890404, 4.74438619, 6.14292792]
    #     # beta = [-6.94090558,  1.01370383,  2.59478602,  2.22505603,  3.73137599,  2.18500756, 6.20478413,  4.70501446,  6.19797428]
    #     # beta = [-6.42521552, 0.97043089, -0.02856156, -0.08602916, 4.47790025, 2.98275166, 4.69470521, 2.82686894, 4.21941468]
    #     # beta = [-6.65183061, 1.16847774, 0.79959162, 4.69449494, 4.89787159, 2.10014087, 4.30171303]
    # elif sys.argv[2] == 'android':
    #     beta = [-4.39166128, -1.68653272, 1.2360036, 6.12467625, 7.34842325, 4.34031723, 6.00230661]
    #     # beta = [-4.39166128, -1.68653272, 1.2360036, 6.12467625, 5.5, 4.34031723, 6.00230661]
    #     # beta = [-4.39166128, -1.68653272, 1.2360036, 6.12467625, 6.0, 4.34031723, 6.00230661]
    # beta = np.concatenate((clf.intercept_, clf.coef_[0]))
    X_test_part = sm.add_constant(x)
    test_prob = logitModel(X_test_part, beta)
    test_pred = (test_prob >= alpha) + 0
    return test_prob, test_pred


def logitModel(X, beta):
    return 1.0 / (1.0 + np.exp(-np.dot(X, beta)))