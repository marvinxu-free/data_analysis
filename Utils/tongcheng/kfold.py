import numpy as np
from dfp_model.machine_learning.logistic import train_logistic, test_logistic
from dfp_model.prec_rec import do_online_prec_rec
from sklearn.model_selection import KFold

def doKFold(X, y, cols, alpha):
    """
    This function is to do k-fold cross-validation.

    Parameter
    ---------
    X: the dataframe including the independent variables
    y: the dataframe of label
    alpha: the threshold for label

    Returns
    -------
    prec: the mean precision of k-fold test
    rec:  the mean recall of k-fold test

    """
    kf = KFold(n_splits=10)
    prec = []
    rec = []
    tpr = []
    fpr = []
    for train_index, test_index in kf.split(X):
        X_train, X_test = X.loc[train_index], X.loc[test_index]
        y_train, y_test = y[train_index], y[test_index]
        clf = train_logistic(X_train, y_train, cols)
        test_prob, test_pred = test_logistic(X_test, cols, clf, alpha)
        X_test["prob"] = test_prob
        X_test["pred"] = test_pred
        PREC, REC, _, _, _, _, TPR, FPR = do_online_prec_rec(X_test)
        # PREC, REC, precision_lower, recall_lower, precision_upper, recall_upper, TPR, FPR = prec_and_recall(test_pred, y_test)
        prec.append(PREC)
        rec.append(REC)
        tpr.append(TPR)
        fpr.append(FPR)
    return np.mean(prec), np.mean(rec), np.min(prec), np.min(rec), np.max(prec), np.max(rec), np.mean(tpr), np.mean(fpr)