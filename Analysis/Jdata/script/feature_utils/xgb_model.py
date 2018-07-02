# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/9
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function, division
import xgboost as xgb
from xgboost import plot_importance
from Utils.common.gridCV import gridCV
from sklearn.metrics import classification_report
import numpy as np
import pandas as pd

from Params.cv_params import cv_params
from Params.xgb_cv_params import xgb_base_params, xgb_jd_params
from Pic.confusion import plot_confusion_matrix_sns
from Utils.common.splitData import split_np
from Pic.draw_roc import pic_roc_cmp
from Pic.pr_curve import pic_pr_cmp, pic_pr_cmp_threshold, pic_pr_step_time, pic_pr_travel_time
from Pic.pic_fscore import pic_fbeta_cmp
from Pic.ml_feature_importance import plot_xgb_importance
from Pic.hist import pic_label
import os
import errno
from Params.path_params import Params_path, Analysis_path
from Params.jd_params import *
import inspect


def jd_xgb_c(X_train, X_test, y_train, y_test, missing=0, post_fix='part1'):
    print("image path {0}".format(v2_img_path))

    sum_pos = np.where(y_train == 1)[0].shape[0]
    sum_neg = np.where(y_train == 0)[0].shape[0]
    scale_ratio = sum_pos / sum_neg
    xgb_base_params['scale_pos_weight'] = scale_ratio
    xgb_base_params['missing'] = missing
    # y_train = y_train.reshape(-1,1)
    # y_test = y_test.reshape(-1,1)
    y_train_cv = y_train

    xgb_base_params['silent'] = 0

    params_path = "{0}/grid_search_params/{1}_{2}.json".format(Analysis_path, inspect.stack()[0][3], post_fix)
    _params, _ = gridCV(clf=xgb.XGBClassifier, X=X_train, y=y_train_cv,
                        base_params=xgb_base_params, all_test_params=xgb_jd_params,
                        cv_parmas=cv_params, model_path=params_path)
    clf = xgb.XGBClassifier(**_params)
    clf_1 = clf.fit(X_train, y_train)
    y_pred = clf_1.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['0', '1']))
    y_pred_prob = clf_1.predict_proba(X_test)

    # check and create file
    print("image path {0}".format(v2_img_path))
    if not os.path.exists(v2_img_path):
        try:
            os.makedirs(v2_img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise

    ## plot feature importance for xgb
    plot_xgb_importance(model=clf_1, fname="{0}/feature_importance.png".format(v2_img_path))
    ## plot confusion matrix
    plot_confusion_matrix_sns(y_pred=y_pred, y_test=y_test, title='JD Rsik Login Confusion Matrix', threshold=0.5,
                              path=v2_img_path)

    cmp_pr = []
    cmp_pr.extend([
        ('JD Rsik Login', y_test, y_pred_prob[:, 1]),
    ])

    # draw roc on xgboost
    pic_pr_cmp(title="JD Rsik Login PR Curve", file_path=v2_img_path, y_data=cmp_pr)
    pic_pr_cmp_threshold(title="JD Rsik Login Pecision & Recall", file_path=v2_img_path, y_data=cmp_pr)
    pic_roc_cmp(title="JD Rsik Login ROC", file_path=v2_img_path, y_data=cmp_pr)
    ##f1 score
    best_threshold = pic_fbeta_cmp(title="JD Rsik Login Fbeta-Score", file_path=v2_img_path, y_data=cmp_pr, beta=0.1)
