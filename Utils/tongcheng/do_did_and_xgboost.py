# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from prec_rec import prec_and_recall
import xgboost as xgb
import numpy as np


def do_did_and_xgboost(test_set,cols,mode='app-app'):
    """
    this file used to combine xgboost and did which means anny one predict 1 as effective
    :param test_set:
    :param cols:
    :param mode:
    :return:
    """
    test_copy = test_set.copy(deep=True)
    prec_list = []
    rec_list = []
    alpha_list = np.arange(0,1.0,0.01)
    tpr_list = []
    fpr_list = []
    fscore_list = []
    # evaluate precision and recall  with different alpha
    data = test_copy
    data.reset_index(inplace=True)
    data = data.drop(["index"],axis=1)
    #load xgboost
    bst = xgb.Booster({'nthread': 4})  # init model
    bst.load_model('{0}_xgb.model'.format(mode))  # load data
    x_xgb = xgb.DMatrix(data[cols].values.tolist())
    y_prob_xgb = bst.predict(x_xgb)
    def get_real_pred(row):
        if row['dfp_pred_did'] == 1 or row['dfp_pred_xg'] == 1:
            return 1
        else:
            return 0
    for alpha in alpha_list:
        data["dfp_pred_did"] = data.did_label
        data["dfp_pred_xg"] = (y_prob_xgb >= alpha) + 0
        dfp_pred = data.apply(lambda x:get_real_pred(x),axis=1).reset_index(name="dfp_pred")
        precision, recall, _, _, _, _, TPR, FPR, FSCORE = prec_and_recall(dfp_pred["dfp_pred"].values, data.label.values)
        prec_list.append(precision)
        rec_list.append(recall)
        tpr_list.append(TPR)
        fpr_list.append(FPR)
        fscore_list.append(FSCORE)
    prec_series = []
    rec_series = []
    time_series = []
    # prec_series, rec_series, time_series = prc_by_time(df_copy)
    return alpha_list, prec_list, rec_list, fscore_list, tpr_list, fpr_list, time_series, prec_series, rec_series


def do_did(test_set):
    """
    use did_label to get precision and recall
    :param test_set:
    :return:
    """
    test_copy = test_set.copy(deep=True)
    prec_list = []
    rec_list = []
    alpha_list = np.arange(0,1.0,0.01)
    tpr_list = []
    fpr_list = []
    fscore_list = []
    # evaluate precision and recall  with different alpha
    data = test_copy
    data.reset_index(inplace=True)
    data = data.drop(["index"],axis=1)
    for alpha in alpha_list:
        data["dfp_pred_did"] = data.did_label
        precision, recall, _, _, _, _, TPR, FPR, FSCORE = prec_and_recall(data["dfp_pred_did"].values, data.label.values)
        prec_list.append(precision)
        rec_list.append(recall)
        tpr_list.append(TPR)
        fpr_list.append(FPR)
        fscore_list.append(FSCORE)
    prec_series = []
    rec_series = []
    time_series = []
    # prec_series, rec_series, time_series = prc_by_time(df_copy)
    return alpha_list, prec_list, rec_list, fscore_list, tpr_list, fpr_list, time_series, prec_series, rec_series
