import pickle
import numpy as np
from prec_rec import prec_and_recall


def do_evaluation(df, filename):
    alpha_list, prec, rec = evaluate_result(df)  # dfp_only
    print prec
    print rec
    # pickle.dump(alpha_list, open("alpha_" + filename, "wb"))
    # pickle.dump(prec, open("prec_" + filename, "wb"))
    # pickle.dump(rec, open("rec_" + filename, "wb"))
    # prc_curve_cmp(alpha_list, prec1, rec1)


def evaluate_result(score, y):
    tpr = []
    fpr = []
    prec = []
    rec = []
    fscore = []
    a = np.arange(100) / 100.0
    # b = np.arange(990, 1000) / 1000.0
    # alpha_list = np.concatenate((a, b))
    alpha_list = a
    for alpha in alpha_list:
        print "alpha:", alpha
        pred = (score >= alpha) + 0
        # PREC, REC, precision_lower, recall_lower, precision_upper, recall_upper, TPR = do_online_prec_rec(df)
        PREC, REC, _, _, _, _, TPR, FPR, FSCORE = prec_and_recall(pred, y)
        tpr.append(TPR)
        fpr.append(FPR)
        prec.append(PREC)
        rec.append(REC)
        fscore.append(FSCORE)
    return alpha_list, prec, rec, fscore, tpr, fpr


def prc_by_time(df):
    df_copy = df.copy(deep=True)
    prec_series = []
    rec_series = []
    time_series = []
    # print df_copy.head(3)
    df_copy.reset_index(inplace=True)
    df_copy.drop("index", axis=1, inplace=True)
    df_copy["pred"] = map(lambda x: 1 if x >= 0.9 else 0, df_copy["score"])
    for i in np.arange(1, 24*60/30+1):
        # test_set["proba"] = y_prob
        sub_df_copy = df_copy[(df_copy.ts_diff <= i * 30 * 60) & (df_copy.ts_diff > (i-1) * 30 * 60)]
        if len(sub_df_copy) > 0:
            prec, rec, _, _, _, _, _, _, _ = prec_and_recall(sub_df_copy["pred"].values, sub_df_copy["label"].values)
        else:
            prec = prec_series[-1]
            rec = rec_series[-1]
        prec_series.append(prec)
        rec_series.append(rec)
        time_series.append(i * 30)
    return prec_series, rec_series, time_series
