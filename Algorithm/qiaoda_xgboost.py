# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function, division

import json
import xgboost as xgb
from Utils.common.gridCV import gridCV
from sklearn.metrics import classification_report

from Params.cv_params import cv_params
from Params.xgb_cv_params import xgb_base_params, xgb_qiaoda_params
from Pic.confusion import plot_confusion_matrix_sns
from Utils.common.splitData import splitData
from Pic.draw_roc import pic_roc_cmp
from Pic.pr_curve import pic_pr_cmp,pic_pr_cmp_threshold,pic_pr_step_time,pic_pr_travel_time
from Pic.pic_fscore import pic_fbeta_cmp
from Pic.hist import pic_label
import os
import errno
from Params.path_params import Data_path, Params_path
import inspect

img_save_path = "{0}/image_qiaoda_{1}".format(Data_path,"{0}")


def qiaoda_xgb(df, missing=-9.9999, postfix='part1'):
    img_path = img_save_path.format(postfix)
    print("image path {0}".format(img_path))
    df_train, df_test = splitData(df, 0.7)
    X_cols = df.columns.difference(['label','IMEI'])
    X_train = df_train[X_cols]
    X_test = df_test[X_cols]
    y_train = df_train.ix[:, df_train.columns == 'label']
    y_test = df_test.ix[:, df_test.columns == 'label']
    print("used columns is {0}".format(X_cols))

    sum_pos = y_train.loc[y_train.label == 1].shape[0]
    sum_neg = y_train.loc[y_train.label == 0].shape[0]
    scale_ratio = sum_pos / sum_neg
    xgb_base_params['scale_pos_weight'] = scale_ratio
    # xgb_base_params['missing'] = missing

    y_train_cv = y_train['label']
    xgb_base_params['silent'] = 0

    params_path = "{0}/{1}_{2}.json".format(Params_path,inspect.stack()[0][3], postfix)
    _params, _ = gridCV(clf=xgb.XGBClassifier, X=X_train, y=y_train_cv,
                            base_params=xgb_base_params, all_test_params=xgb_qiaoda_params,
                            cv_parmas=cv_params,model_path=params_path)

    clf = xgb.XGBClassifier(**_params)
    clf_1 = clf.fit(X_train, y_train)
    y_pred = clf_1.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['0', '1']))
    y_pred_prob = clf_1.predict_proba(X_test)

    # check and create file
    img_path = os.path.realpath(img_save_path.format(postfix))
    print("image path {0}".format(img_path))
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise

    pic_label(df=df_train, col='label', title="QiaoDa Positive-Negative Data Ratio", file_path=img_path)
    ## plot confusion matrix
    plot_confusion_matrix_sns(y_pred=y_pred,y_test=y_test,title='QiaoDa Confusion Matrix',threshold=0.5,path=img_path)

    cmp_pr = []
    cmp_pr.extend([
        ('Qiaoda', y_test, y_pred_prob[:, 1]),
    ])

    # draw roc on xgboost
    pic_pr_cmp(title="QiaoDa PR Curve", file_path=img_path, y_data=cmp_pr)
    pic_pr_cmp_threshold(title="QiaoDa Pecision & Recall", file_path=img_path, y_data=cmp_pr)
    pic_roc_cmp(title="QiaoDa ROC", file_path=img_path, y_data=cmp_pr)
    ##f1 score
    best_threshold=pic_fbeta_cmp(title="QiaoDa Fbeta-Score", file_path=img_path, y_data=cmp_pr)





