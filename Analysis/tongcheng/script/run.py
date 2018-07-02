# coding=utf-8
import sys

import numpy as np
import pandas as pd
from machine_learning.logistic import do_logistic
from sklearn.model_selection import train_test_split
from src.main.python.dfp_model.preprocess import do_data_parser, do_correct_label, add_category

from Utils.tongcheng.data_reader import desc_data
from Utils.tongcheng.data_reader import read_data

if sys.argv[2] == 'ios':
    if sys.argv[3] == 'boottime':
        # cat_threshold = [-10000, -22, -3, 11, 23, 10000]
        # chosen_cols = ["boottime -3~11", "boottime 11~23", "uaLv3", "uaLv5", "uaLv6"]
        # model_beta = [-8.5529, -1.59, 4.5875, 10.3380, 10.0, 9.8633]  # online params
        # cols = ["boottime -4~0", "boottime 0~5.5", "boottime 5.5~5.7", "boottime 5.7~5.9", "boottime 5.9~10.9", "boottime 10.9~22.6", "boottime 22.6~24.2", "boottime 24.2~31", "uaLv3", "uaLv5", "uaLv6"]
        # cols = ["boottime -4~0", "boottime 5.5~5.7", "boottime 5.7~5.9", "boottime 10.9~22.6", "boottime 22.6~24.2", "uaLv3", "uaLv5", "uaLv6"]
        # model_beta = [-6.94090558,  1.01370383,  2.59478602,  2.22505603,  3.73137599,  2.18500756, 6.20478413,  4.70501446,  6.19797428]
        # model_beta = [-6.42521552, 0.97043089, -0.02856156, -0.08602916, 4.47790025, 2.98275166, 4.69470521, 2.82686894, 4.21941468]
        # cols = ["boottime -4~6.3", "boottime 6.3~10.9", "boottime 10.9~28", "uaLv3", "uaLv5", "uaLv6"]  # classify according to non-proxy data
        # model_beta = [-6.65183061, 1.16847774, 0.79959162, 4.69449494, 4.89787159, 2.10014087, 4.30171303]
        cat_threshold = [-1000000, -5.8, -5.5, 0.0, 6.3, 10.9, 1000000]
        chosen_cols = [
            "boottime 0.0~6.3", "boottime 6.3~10.9",
            "boottime 10.9~1000000", "uaLv3", "uaLv5", "uaLv6"]
        # model_beta = [-4.76360748, -2.64234033, -1.62958029,  5.61885778,  3.4593325 ,  3.4593325, 3.5158444 ]
        # model_beta = [-4.76360748, -2.64234033, -1.62958029, 5.0, 3.4593325, 3.4593325, 3.5158444]  # final online params
        model_beta = pd.DataFrame(
            {"intercept": [-4.76360748], "boottime 0.0~6.3": [-2.64234033], "boottime 6.3~10.9": [-1.62958029],
             "boottime 10.9~1000000": [5.0], "uaLv3": [3.4593325], "uaLv5": [3.4593325], "uaLv6": [3.5158444]})
        # model_beta = [-4.76360748, -2.64234033, 5.0, -1.62958029, 3.4593325, 3.4593325]
    elif sys.argv[3] == 'slope':
        cat_threshold = [-1000000, 0.0, 0.906551360842, 1.06534147176, 276.786553515, 726.000469512, 1000000]
elif sys.argv[2] == 'android':
    if sys.argv[3] == 'boottime':
        # cat_threshold = [-10000, -22, -2, -1, 1, 11, 10000]
        # chosen_cols = ["boottime -22~-2", "boottime -1~1", "boottime 11~10000", "uaLv3", "uaLv5", "uaLv6"]
        # model_beta = [-4.39166128, -1.68653272, 1.2360036, 6.12467625, 7.34842325, 4.34031723, 6.00230661]  # online params
        # model_beta = [-4.39166128, -1.68653272, 1.2360036, 6.12467625, 5.5, 4.34031723, 6.00230661]  # perform best
        # beta = [-4.39166128, -1.68653272, 1.2360036, 6.12467625, 6.0, 4.34031723, 6.00230661]
        cat_threshold = [-1000000, -8.0, -5.9, -5.0, -4.4, -1.8, -1.7, -1.6, -1.4, -0.1, 0.0, 12.6, 14.2, 1000000]
        # cat_threshold = [-29, -8.2, -5.7, -5.1, -4.4, -2.8, -1.8, -1.7, -1.4, -0.2, 0.0, 12.6, 14.2, 35]
        # chosen_cols = [
        # "boottime -29~-8.2",
        # "boottime -8.2~-5.7",
        # "boottime -5.7~-5.1",
        # "boottime -5.1~-4.4",
        # "boottime -4.4~-2.8",
        # "boottime -2.8~-1.8",
        # "boottime -1.8~-1.7",
        # "boottime -1.7~-1.4",
        # "boottime -1.4~-0.2",
        #               "boottime -0.2~0.0",
        #               "boottime 0.0~12.6",
        #               "boottime 12.6~14.2", "boottime 14.2~35","uaLv3", "uaLv5",
        # "uaLv6"]

        # chosen_cols = [
        #     "boottime -1000000~-8.0",
        #     "boottime -8.0~-5.9",
        #     "boottime -5.9~-5.0",
        #     "boottime -5.0~-4.4",
        #     "boottime -1.8~-1.7",
        #     "boottime -1.7~-1.6",
        #     "boottime -1.6~-1.4",
        #     "boottime -1.4~-0.1",
        #     "boottime -0.1~0.0",
        #     "boottime 0.0~12.6",
        #     "boottime 12.6~14.2", "boottime 14.2~1000000", "uaLv3", "uaLv5",
        #     "uaLv6"
        # ]
        chosen_cols = [
            # "boottime -1000000~-8.0",
            # "boottime -5.9~-5.0",
            # "boottime -5.0~-4.4",
            # "boottime -1.6~-1.4",
            "boottime -0.1~0.0",
            "boottime 0.0~12.6",
            "boottime 12.6~14.2", "boottime 14.2~1000000", "uaLv3", "uaLv5",
            "uaLv6"
        ]
        model_beta = pd.DataFrame({"intercept": [-5.87053327], "boottime -0.1~0.0": [2.62970582],
                                   "boottime 0.0~12.6": [-0.45683986],
                                   "boottime 12.6~14.2": [3.04138293], "boottime 14.2~1000000": [7.97964505],
                                   "uaLv3": [6.50412494], "uaLv5": [4.78467716],
                                   "uaLv6": [6.50412494]})
        # model_beta = [-5.87053327, 2.62970582, -0.45683986, 3.04138293, 7.97964505, 6.50412494,
        # 4.78467716, 7.47111938]
        #  4.78467716, 6.50412494]   # final online params
        # cat_threshold = [-10000.0, -9.2, -7.5, -6.3, -4.4, -3.4, -2.4, -1.5, -0.4, 0.5, 1.1, 2.0, 4.1, 10000.0]
    elif sys.argv[3] == 'slope':
        cat_threshold = [-1000000, 0.0, 0.0953086226746, 0.164564280458, 2745.80703178, 1000000]
else:
    print "argument num should be 3, 1 given."


def splitData(df, ratio):
    """
    """
    positiveData = df[df["label"] == 1].copy(deep=True)
    _positiveTrain, _positiveTest = train_test_split(positiveData, train_size=ratio)
    negativeData = df[df["label"] == 0].copy(deep=True)
    _negativeTrain, _negativeTest = train_test_split(negativeData, train_size=ratio)
    train_set = pd.concat([_positiveTrain, _negativeTrain])
    test_set = pd.concat([_positiveTest, _negativeTest])
    return train_set, test_set


def run(df):
    """
    This is the running function.

    Parameter
    --------
    df: the dataframe to run model

    """
    global chosen_cols
    # boottime
    # if sys.argv[2] == 'ios':
    #     cols = ["boottime -3~11", "boottime 11~23", "uaLv3", "uaLv5", "uaLv6"]
    #     # cols = ["boottime -4~0", "boottime 0~5.5", "boottime 5.5~5.7", "boottime 5.7~5.9", "boottime 5.9~10.9", "boottime 10.9~22.6", "boottime 22.6~24.2", "boottime 24.2~31", "uaLv3", "uaLv5", "uaLv6"]
    #     # cols = ["boottime -4~0", "boottime 5.5~5.7", "boottime 5.7~5.9", "boottime 10.9~22.6", "boottime 22.6~24.2", "uaLv3", "uaLv5", "uaLv6"]
    #     # cols = ["boottime -4~6.3", "boottime 6.3~10.9", "boottime 10.9~28", "uaLv3", "uaLv5", "uaLv6"]  # classify according to non-proxy data
    # elif sys.argv[2] == 'android':
    #     cols = ["boottime -22~-2", "boottime -1~1", "boottime 11~10000", "uaLv3", "uaLv5", "uaLv6"]
    # else:
    #     print "argument num should be 2, 1 given."
    cols = np.intersect1d(df.columns.tolist(), chosen_cols)
    # train_set, test_set = splitData(df, 0.6)
    do_logistic(df, df, cols, 0.7)

    # do_logistic(df, df, cols, 0.7)

    # if sys.argv[2] == 'ios':
    #     for i in np.arange(50, 95):
    #         alpha = i / 100.0
    #         print "alpha:", alpha
    #         train_set, test_set = splitData(df, 0.6)
    #         #do_logistic(train_set, test_set, cols, alpha)
    #         do_logistic(df, df, cols, alpha)
    # elif sys.argv[2] == 'android':
    #     for i in np.arange(50, 95):
    #         alpha = i / 100.0
    #         print "alpha:", alpha
    #         train_set, test_set = splitData(df, 0.6)
    #         # do_logistic(df, df["label"].values, cols, alpha)
    #         #do_logistic(train_set, test_set, cols, alpha)
    #         do_logistic(df, df, cols, alpha)
    # else:
    #     print "argument num should be 2, 1 given."

    # slope

    # modeling all variables
    # cols = ["uaLv3","uaLv5","uaLv6", "ipLv1","ipLv2","ipLv3","cat 0.0~0.906551360842","cat 0.906551360842~1.06534147176","cat 1.06534147176~276.786553515", "cat 276.786553515~726.000469512","cat 726.000469512~1000000"]
    # doLogistic(df, df["label"], cols, 0.86)

    # modeling part variables
    # cols = ["uaLv3","uaLv5","uaLv6","slope 0.0~0.906551360842","slope 0.906551360842~1.06534147176","slope 1.06534147176~276.786553515", "slope 276.786553515~726.000469512","slope 726.000469512~1000000"]
    # doLogistic(df, df["label"], cols, 0.86)
    # doSVM(df, df["label"], cols)
    # pickle.dump(df, open("rawdata","wb"))
    # DNN(df, df["label"], cols)
    '''
    # keep slope continuous
    cols = ["uaLv3","uaLv5","uaLv6", "ipLv1","ipLv2","ipLv3","slope", "os_sig_equals"]
    doLogistic(df, df["label"], cols, 0.86)
    # keep slope continuous and consider os_signature
    cols = ["uaLv3","uaLv5","uaLv6","slope","os_sig_equals"]
    doLogistic(df, df["label"], cols, 0.86)
    # keep slope discrete and consider os_signature
    cols = ["uaLv3","uaLv5","uaLv6", "ipLv1","ipLv2","ipLv3","cat 0.0~0.906551360842","cat 0.906551360842~1.06534147176","cat 1.06534147176~276.786553515", "cat 276.786553515~726.000469512","cat 726.000469512~1000000", "os_sig_equals"]
    doLogistic(df, df["label"], cols, 0.86)
    # keep slope discrete and consider os_signature
    cols = ["uaLv3","uaLv5","uaLv6","cat 0.0~0.906551360842","cat 0.906551360842~1.06534147176","cat 1.06534147176~276.786553515", "cat 276.786553515~726.000469512","cat 726.000469512~1000000","os_sig_equals"]
    doLogistic(df, df["label"], cols, 0.86)
    '''
    # cols = ["uaLv3","uaLv5","uaLv6","slope 0.0~0.906551360842","slope 0.906551360842~1.06534147176","slope 1.06534147176~276.786553515", "slope 276.786553515~726.000469512","slope 726.000469512~1000000"]

    # prec = []
    # rec = []
    # min_prec = []
    # min_rec = []
    # max_prec = []
    # max_rec = []
    # tpr = []
    # fpr = []
    # alpha_list = []
    # for i in np.arange(0, 1001):
    #     alpha = i / 1000.0
    #     alpha_list.append(alpha)
    #     print "alpha: ", alpha
    #     # PREC, REC, min_PREC, min_REC, max_PREC, max_REC, TPR, FPR = doKFold(df, df["label"].values, cols, alpha)
    #     PREC, REC, min_PREC, min_REC, max_PREC, max_REC, TPR, FPR = do_logistic(df.copy(deep=True), df.copy(deep=True), cols, alpha)
    #     prec.append(PREC)
    #     rec.append(REC)
    #     min_prec.append(min_PREC)
    #     min_rec.append(min_REC)
    #     max_prec.append(max_PREC)
    #     max_rec.append(max_REC)
    #     tpr.append(TPR)
    #     fpr.append(FPR)
    #     print "precision: ", PREC, min_PREC, max_PREC
    #     print "recall: ", REC, min_REC, max_REC
    # pickle.dump(tpr, open("android_dfp_only_tpr", "wb"))
    # pickle.dump(fpr, open("android_dfp_only_fpr", "wb"))
    # pickle.dump(tpr, open("android_mcid_did_dfp_tpr", "wb"))
    # pickle.dump(fpr, open("android_mcid_did_dfp_fpr", "wb"))

    # prc_curve(alpha_list, prec, rec, min_prec, min_rec, max_prec, max_rec)
    # pickle.dump(alpha_list, open("ios_alpha_list", "wb"))
    # pickle.dump(prec, open("ios_dfp_only_prec", "wb"))
    # pickle.dump(rec, open("ios_dfp_only_rec", "wb"))
    # prc_curve_with_confid(alpha_list, prec, rec, min_prec, min_rec, max_prec, max_rec)


def main():
    fname = sys.argv[1]  # ./data/android_result.json
    df = read_data(fname)
    df = do_correct_label(df)
    df = do_data_parser(df)
    df[["boottime", "ts_diff"]] = df[["boottime", "ts_diff"]].astype(float)
    df[["label", "maxent_id_equals"]] = df[["label", "maxent_id_equals"]].astype(int)
    # get ua dummies, ip dummies and boottime dummies
    ua_dummies = pd.get_dummies(df["ua"])
    ip_dummies = pd.get_dummies(df["ip"])
    ft_name = "boottime"
    df = add_category(df, ft_name)
    cross = pd.crosstab(df["label"], df["boottime_cat"])
    # print cross
    bt_dummies = pd.get_dummies(df[ft_name + "_cat"])
    # print bt_dummies.columns
    # bt_dummies.drop(bt_dummies.columns[0], axis=1, inplace=True)
    new_df = pd.concat([df, ua_dummies, ip_dummies, bt_dummies], axis=1)
    # get slope dummies if slope exists
    if 'slope' in df.columns:
        df[["slope"]] = df[["slope"]].astype(float)
        ft_name = "slope"
        df = add_category(df, ft_name)
        sl_dummies = pd.get_dummies(df[ft_name + "_cat"])
        # sl_dummies.drop(sl_dummies.columns[0], axis=1, inplace=True)
        new_df = pd.concat([new_df, sl_dummies], axis=1)
    # new_df = new_df.drop(["ua", "ip", "boottime", "uaLv2", "uaNoMatch", "ipNoMatch"], axis=1, errors="ignore")
    # pickle.dump(new_df, open("new_train_data","wb"))
    # newDf = pickle.load(open("new_train_data","rb"))
    # new_df = new_df[(new_df.doc_tcpts < 1e9) & (new_df.query_tcpts < 1e9)]
    desc_data(new_df)
    # new_df = new_df[(new_df.query_is_proxy == 'false') & (new_df.doc_is_proxy == 'false')]
    pd.set_option("max_colwidth", 300)
    # print "app data: ", get_event_id_num(new_df, ['MOBILE_APP', 'PC_APP'])
    # print "web data: ", get_event_id_num(new_df, ['MOBILE_WEB', 'PC_WEB'])
    # print "app+web:", len(new_df[((new_df.query_source == 'MOBILE_APP') | (new_df.query_source == 'PC_APP'))
    #                              & ((new_df.doc_source == 'MOBILE_WEB') | (new_df.doc_source == 'PC_WEB'))])
    # print "app+app:", len(new_df[((new_df.query_source == 'MOBILE_APP') | (new_df.query_source == 'PC_APP'))
    #                              & ((new_df.doc_source == 'MOBILE_APP') | (new_df.doc_source == 'PC_APP'))])
    # print "web+web:", len(new_df[((new_df.query_source == 'MOBILE_WEB') | (new_df.query_source == 'PC_WEB'))
    #                              & ((new_df.doc_source == 'MOBILE_WEB') | (new_df.doc_source == 'PC_WEB'))])
    # print "web+app:", len(new_df[((new_df.query_source == 'MOBILE_WEB') | (new_df.query_source == 'PC_WEB'))
    #                              & ((new_df.doc_source == 'MOBILE_APP') | (new_df.doc_source == 'PC_APP'))])
    # print "-----prec and rec from app:\n"
    # did_match(new_df[((new_df.query_source == 'MOBILE_APP') | (new_df.query_source == 'PC_APP'))
    #                              & ((new_df.doc_source == 'MOBILE_APP') | (new_df.doc_source == 'PC_APP'))])
    # print "-----prec and rec from the same source: \n"
    # test_df = mcid_match(new_df[(new_df.query_source == new_df.doc_source)])
    # print test_df[(test_df.label==1) & (test_df.pred==0)][["query_did", "doc_did", "label", "pred"]].head()
    pd.set_option("max_colwidth", 300)
    # print new_df[["query_did", "doc_did"]].head()
    # print "-----prec and rec from all data: \n"
    # did_match(new_df)
    # mcid_match(new_df)
    # new_df = mcid_match(new_df)
    # new_df = did_match(new_df)
    run(new_df)


if __name__ == "__main__":
    # do_plot_prc()
    # do_plot_roc()
    main()
    # run_online()
    # unittest.main()







