# -*- coding: utf-8 -*-
# Project: maxent-ml
# Author: chaoxu create this file
# Time: 2017/9/27
# Company : Maxent
# Email: chao.xu@maxent-inc.com
'''
this file is used to get best paramter used kfold
'''
from __future__ import print_function, division
from sklearn.model_selection import GridSearchCV
import os
import json


def gridCV(clf, X, y, base_params, all_test_params, cv_parmas, model_path=None):
    """
    this function used to do gridCV search for all possible parameter for clf.
    first check gridCV has done or not, if has done already, will use save model parameters
    :param clf:
    :param X:
    :param y:
    :param base_params:
    :param all_test_params:
    :param cv_parmas:
    :param model_path:
    :return:
    """
    if os.path.exists(path=model_path):
        with open(model_path, "r") as f:
            best_params = json.load(f)
            clf_cv = None
        print("get best parameters is {0}".format(best_params))
        return best_params, clf_cv
    else:
        best_params = {}
        best_params.update((base_params))
        clf_cv = None
        for k,v in all_test_params.items():
            test_params = {k:v}
            # print("find best params of {0}".format(test_params))
            clf_cv = GridSearchCV(estimator=clf(**best_params),param_grid=test_params,**cv_parmas)
            clf_cv.fit(X, y)
            # means = clf_cv.cv_results_['mean_test_score']
            # stds = clf_cv.cv_results_['std_test_score']
            # for mean, std, params in zip(means, stds, clf_cv.cv_results_['params']):
                # print("%0.3f (+/-%0.03f) for %r"
                #       % (mean, std * 2, params))
            best_params.update(clf_cv.best_params_)
        print("get best parameters is {0}".format(best_params))
        if model_path is not None:
            with open(model_path, "w") as f:
                json.dump(obj=best_params, fp=f, sort_keys=True, indent=4)
        return best_params,clf_cv
