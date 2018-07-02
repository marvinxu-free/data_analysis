# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/13
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from sklearn.cross_validation import KFold
import numpy as np
from sklearn.metrics import classification_report

def get_oof(clf, x_train, y_train, x_test,y_test,NFOLDS = 5):
    ntrain = x_train.shape[0]
    ntest = x_test.shape[0]
    kf = KFold(ntrain, n_folds=NFOLDS, random_state=1234)
    oof_train = np.zeros((ntrain,))
    oof_test = np.zeros((ntest,))
    oof_test_skf = np.empty((NFOLDS, ntest))

    print("----")
    for i, (train_index, test_index) in enumerate(kf):
        x_tr = x_train[train_index]
        y_tr = y_train[train_index]
        x_te = x_train[test_index]
        clf.fit(x_tr, y_tr)
        oof_train[test_index] = clf.predict(x_te)
        oof_test_skf[i, :] = clf.predict(x_test)
        print("kfold {0} report".format(i))
        print(classification_report(y_test,oof_test_skf[i, :], target_names=['0', '1']))

    oof_test[:] = oof_test_skf.mean(axis=0)
    return oof_train.reshape(-1, 1), oof_test.reshape(-1, 1)
