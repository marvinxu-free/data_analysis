# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from prec_rec import *
def do_online_dfp(df):
    df_copy = df.copy(deep=True)
    prec_list = []
    rec_list = []
    alpha_list = []
    tpr_list = []
    fpr_list = []
    fscore_list = []
    # evaluate precision and recall  with different alpha
    data = df_copy
    # print data.head(3)
    data.reset_index(inplace=True)
    data.drop("index", axis=1, inplace=True)
    for i in np.arange(0, 100):
        alpha = i*0.01
        # data["dfp_pred"] = map(lambda x: 1 if x >= alpha else 0, data["score"])
        data["dfp_pred"] = (data["score"] >= alpha) + 0
        print "alpha:", alpha
        precision, recall, _, _, _, _, TPR, FPR, FSCORE = prec_and_recall(data["dfp_pred"], data.label)
        prec_list.append(precision)
        rec_list.append(recall)
        alpha_list.append(alpha)
        tpr_list.append(TPR)
        fpr_list.append(FPR)
        fscore_list.append(FSCORE)
    # evaluate precision and recall with different ts-diff(alpha=0.9)
    prec_series = []
    rec_series = []
    time_series = []
    # prec_series, rec_series, time_series = prc_by_time(df_copy)
    return alpha_list, prec_list, rec_list, fscore_list, tpr_list, fpr_list, time_series, prec_series, rec_series

