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
from Params.xgb_cv_params import xgb_base_params, xgb_test_params
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

img_save_path = "{0}/image_dfp_{1}".format(Data_path,"{0}")


def ios_dfp_xg(df, missing=-9.9999, postfix='part1'):
    img_path = img_save_path.format(postfix)
    print("image path {0}".format(img_path))
    df_train, df_test = splitData(df, 0.7)
    X_cols = df.columns.difference(['label','score','event_fly'])
    X_train = df_train[X_cols]
    X_test = df_test[X_cols]
    y_train = df_train.ix[:, df_train.columns == 'label']
    y_test = df_test.ix[:, df_test.columns == 'label']
    y_pred_old = df_test.ix[:, df_test.columns == 'score']
    print("used columns is {0}".format(X_cols))

    sum_pos = y_train.loc[y_train.label == 1].shape[0]
    sum_neg = y_train.loc[y_train.label == 0].shape[0]
    scale_ratio = sum_pos / sum_neg
    xgb_base_params['scale_pos_weight'] = scale_ratio
    xgb_base_params['missing'] = missing

    y_train_cv = y_train['label']

    params_path = "{0}/{1}_{2}.json".format(Params_path,inspect.stack()[0][3], postfix)
    _params, _ = gridCV(clf=xgb.XGBClassifier, X=X_train, y=y_train_cv,
                            base_params=xgb_base_params, all_test_params=xgb_test_params,
                            cv_parmas=cv_params,model_path=params_path)

    clf = xgb.XGBClassifier(**_params)
    xgb_base_params['silent'] = 0
    clf_1 = clf.fit(X_train, y_train)
    clf_1._Booster.save_model('part1.model')
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

    pic_label(df=df_train, col='label', title="dfp label ratio", file_path=img_path)
    ## plot confusion matrix

    plot_confusion_matrix_sns(y_pred=y_pred,y_test=y_test,title='xgboost confusion matrix',threshold=0.5,path=img_path)

    dfp_y_pred = y_pred_old.score.values
    dfp_y_pred[dfp_y_pred < 0.9] = 0
    dfp_y_pred[dfp_y_pred >= 0.9] = 1
    plot_confusion_matrix_sns(y_pred=dfp_y_pred,y_test=y_test,title='dfp confusion matrix',threshold=0.9,path=img_path)

    cmp_pr = []
    cmp_pr.extend([
        ('xgboost',y_test, y_pred_prob[:,1]),
        ('dfp',y_test, y_pred_old),
    ])

    # draw roc on xgboost
    pic_roc_cmp(title="ROC xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    #draw pr compatation betweew xgboost and old model
    pic_pr_cmp(title="PR Curve xgboost vs dfp", file_path=img_path, y_data=cmp_pr)
    pic_pr_cmp_threshold(title="Pecision and Recall xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    #draw f1 score between xgboost and dfp model
    best_threshold=pic_fbeta_cmp(title="fbeta_score xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    pr_travel_time = []
    for k,v in best_threshold.items():
        if k == 'xgboost':
            y_pred_label1_prob  = y_pred_prob[:,1]
            y_pred_label1_prob[y_pred_label1_prob < v] = 0
            y_pred_label1_prob[y_pred_label1_prob >= v] = 1
            pr_travel_time.append(('xgboost', y_test.label.values, y_pred_label1_prob, df_test['event_fly'].values, v))
        else:
            pr_travel_time.append(('dfp', y_test.label.values, dfp_y_pred, df_test['event_fly'].values,0.9))

    pic_pr_step_time(title="precision and recall walk step",
                     file_path=img_path,
                     delta=100,
                     y_data=pr_travel_time
                     )
    pic_pr_travel_time(title="precision and recall walk increase",
                     file_path=img_path,
                     delta=100,
                     y_data=pr_travel_time
                     )


def ios_dfp_p2(df,df_test,missing=-6.666,postfix='part2'):
    """
    this function is used to do xgboost on re-balance data set
    and test model on normal data
    :param df: train data set, which is re-balanced
    :param df_test: normal data set
    :param missing: xgboost missing value
    :return:
    """
    # X_cols = df.columns.difference(['label','score'])
    X_cols = df.columns.difference(['label','score','event_fly'])
    X_train = df[X_cols]
    X_test = df_test[X_cols]
    y_train = df.ix[:, df.columns == 'label']
    y_test = df_test.ix[:, df_test.columns == 'label']
    y_pred_old = df_test.ix[:, df_test.columns == 'score']

    sum_pos = y_train.loc[y_train.label == 1].shape[0]
    sum_neg = y_train.loc[y_train.label == 0].shape[0]
    scale_ratio = sum_pos / sum_neg
    xgb_base_params['scale_pos_weight'] = scale_ratio
    xgb_base_params['missing'] = missing

    y_train_cv = y_train['label']
    _params, clf_0 = gridCV(clf=xgb.XGBClassifier, X=X_train, y=y_train_cv,
                           base_params=xgb_base_params, all_test_params=xgb_test_params, cv_parmas=cv_params)
    y_pred = clf_0.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['0', '1']))
    y_pred_prob = clf_0.predict_proba(X_test)

    # check and create file
    img_path = img_save_path.format(postfix)
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    ## plot confusion matrix

    plot_confusion_matrix_sns(y_pred=y_pred,y_test=y_test,title='xgboost normal data confusion matrix',threshold=0.5,path=img_path)

    dfp_y_pred = y_pred_old.score.values
    dfp_y_pred[dfp_y_pred < 0.9] = 0
    dfp_y_pred[dfp_y_pred >= 0.9] = 1
    plot_confusion_matrix_sns(y_pred=dfp_y_pred,y_test=y_test,title='dfp normal data confusion matrix',threshold=0.9,path=img_path)

    cmp_pr = []
    cmp_pr.extend([
        ('xgboost',y_test, y_pred_prob[:,1]),
        ('dfp',y_test, y_pred_old),
    ])
    # draw roc on xgboost
    pic_roc_cmp(title="ROC normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)
    #draw pr compatation betweew xgboost and old model
    pic_pr_cmp(title="PR Curve normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)
    pic_pr_cmp_threshold(title="Pecision and Recall normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    #draw f1 score between xgboost and dfp model
    best_threshold = pic_fbeta_cmp(title="fbeta_score normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    pr_travel_time = []
    for k,v in best_threshold.items():
        if k == 'xgboost':
            y_pred_label1_prob  = y_pred_prob[:,1]
            y_pred_label1_prob[y_pred_label1_prob < v] = 0
            y_pred_label1_prob[y_pred_label1_prob >= v] = 1
            pr_travel_time.append(('xgboost', y_test.label.values, y_pred_label1_prob, df_test['event_fly'].values, v))
        else:
            pr_travel_time.append(('dfp', y_test.label.values, dfp_y_pred, df_test['event_fly'].values,0.9))

    pic_pr_step_time(title="normal precision and recall walk step",
                     file_path=img_path,
                     delta=100,
                     y_data=pr_travel_time
                     )
    pic_pr_travel_time(title="normal precision and recall walk increase",
                       file_path=img_path,
                       delta=100,
                       y_data=pr_travel_time
                       )


def ios_dfp_p4(df,df_test,missing=-6.666,postfix='part3'):
    """
    this function is used to do xgboost on re-balance data set
    and test model on normal data
    :param df: train data set, which is re-balanced
    :param df_test: normal data set
    :param missing: xgboost missing value
    :return:
    """
    # X_cols = df.columns.difference(['label','score'])
    X_cols = df.columns.difference(['label','score','event_fly'])
    X_train = df[X_cols]
    X_test = df_test[X_cols]
    y_train = df.ix[:, df.columns == 'label']
    y_test = df_test.ix[:, df_test.columns == 'label']
    y_pred_old = df_test.ix[:, df_test.columns == 'score']

    sum_pos = y_test.loc[y_test.label == 1].shape[0]
    sum_neg = y_test.loc[y_test.label == 0].shape[0]
    scale_ratio = sum_pos / sum_neg
    xgb_base_params['scale_pos_weight'] = scale_ratio
    xgb_base_params['missing'] = missing

    y_train_cv = y_train['label']
    _params, clf_0 = gridCV(clf=xgb.XGBClassifier, X=X_train, y=y_train_cv,
                            base_params=xgb_base_params, all_test_params=xgb_test_params, cv_parmas=cv_params)
    y_pred = clf_0.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['0', '1']))
    y_pred_prob = clf_0.predict_proba(X_test)

    # check and create file
    img_path = img_save_path.format(postfix)
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    ## plot confusion matrix

    plot_confusion_matrix_sns(y_pred=y_pred,y_test=y_test,title='xgboost normal data confusion matrix',threshold=0.5,path=img_path)

    dfp_y_pred = y_pred_old.score.values
    dfp_y_pred[dfp_y_pred < 0.9] = 0
    dfp_y_pred[dfp_y_pred >= 0.9] = 1
    plot_confusion_matrix_sns(y_pred=dfp_y_pred,y_test=y_test,title='dfp normal data confusion matrix',threshold=0.9,path=img_path)

    cmp_pr = []
    cmp_pr.extend([
        ('xgboost',y_test, y_pred_prob[:,1]),
        ('dfp',y_test, y_pred_old),
    ])
    # draw roc on xgboost
    pic_roc_cmp(title="ROC normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)
    #draw pr compatation betweew xgboost and old model
    pic_pr_cmp(title="PR Curve normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)
    pic_pr_cmp_threshold(title="Pecision and Recall normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    #draw f1 score between xgboost and dfp model
    best_threshold = pic_fbeta_cmp(title="fbeta_score normal data xgboost vs dfp", file_path=img_path, y_data=cmp_pr)

    pr_travel_time = []
    for k,v in best_threshold.items():
        if k == 'xgboost':
            y_pred_label1_prob  = y_pred_prob[:,1]
            y_pred_label1_prob[y_pred_label1_prob < v] = 0
            y_pred_label1_prob[y_pred_label1_prob >= v] = 1
            pr_travel_time.append(('xgboost', y_test.label.values, y_pred_label1_prob, df_test['event_fly'].values, v))
        else:
            pr_travel_time.append(('dfp', y_test.label.values, dfp_y_pred, df_test['event_fly'].values,0.9))

    pic_pr_step_time(title="normal precision and recall walk step",
                     file_path=img_path,
                     delta=100,
                     y_data=pr_travel_time
                     )
    pic_pr_travel_time(title="normal precision and recall walk increase",
                       file_path=img_path,
                       delta=100,
                       y_data=pr_travel_time
                       )
