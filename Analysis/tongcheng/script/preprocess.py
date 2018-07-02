import numpy as np
import pandas as pd

from machine_learning import gbdt
label_corrected = {"huweiKIW-AL10_phone_ad5.1.1_20170516": "huaweiKIW-AL10_phone_ad5.1.1_20170516",
                   "huawieDIG-AL100_phone_ad6.0_20170525": "huaweiDIG-AL100_phone_ad6.0_20170525",
                   "xiaommiHMNote3_phone_ad5.0.2_20170516": "xiaomiHMNote3_phone_ad5.0.2_20170516",
                   "xiaommi2S_phone_ad4.4.4_20170525": "xiaomi2S_phone_ad4.4.4_20170525",
                   "xiaoimHMNote 2_phone_ad5.0.2_20170516": "xiaomiHMNote 2_phone_ad5.0.2_20170516",
                   "xiaoiNote2_phone_ad6_20170516": "xiaomiNote2_phone_ad6_20170516",
                   "xiaoim2S_phone_ad4.4.4_20170525": "xiaomi2S_phone_ad4.4.4_20170525",
                   "iphone6s_phone_ios9.3.2_20170518": "iphone6s_phone1_ios9.3.2_20170518"}


def do_data_parser(df):
    df = df.apply(lambda x: data_parser(x), axis=1)
    return df


def data_parser(x):
    x["doc_device"] = x["doc_os_sig"]["device"]
    x["doc_brand"] = x["doc_os_sig"]["brand"]
    x["doc_window"] = x["doc_os_sig"]["window"]
    x["doc_ttl"] = x["doc_os_sig"]["ttl"]
    x["doc_wscale"] = x["doc_os_sig"]["wscale"]
    x["query_device"] = x["query_os_sig"]["device"]
    x["query_brand"] = x["query_os_sig"]["brand"]
    x["query_window"] = x["query_os_sig"]["window"]
    x["query_ttl"] = x["query_os_sig"]["ttl"]
    x["query_wscale"] = x["query_os_sig"]["wscale"]
    # x["doc_os_sig_str"] = '&'.join([x["doc_device"], str(x["doc_window"]), str(x["doc_ttl"]), str(x["doc_wscale"])])
    # x["query_os_sig_str"] = '&'.join(
    #     [x["query_device"], str(x["query_window"]), str(x["query_ttl"]), str(x["query_wscale"])])
    # x["os_sig_equals"] = (1 if x["doc_os_sig_str"] == x["query_os_sig_str"] else 0)
    return x


def did_modify(x, event_changed):
    if x["label"] == 0:
        return 0
    else:
        if (event_changed.get(x["doc_event_id"]) == 1) | (event_changed.get(x["query_event_id"]) == 1):
            return 0
        else:
            return 1


def do_correct_label(df):
    def correct_label(x):
        if x in label_corrected.keys():
            return label_corrected.get(x)
        else:
            return x

    df["query_label"] = map(lambda x: correct_label(x), df.query_label)
    df["doc_label"] = map(lambda x: correct_label(x), df.doc_label)
    df["label"] = (df["query_label"] == df["doc_label"]) + 0
    return df


def add_category(x, col_name):
    """
    This function is to discretize the column colName, generating categorical variables.

    Parameter
    ---------
    X: dataframe including column named colName
    colName: column name to be discretized

    Returns
    -------
    X: new dataframe including the categorical variables
    """
    # if sys.argv[2] == 'ios':
    #     if col_name == 'boottime':
    #         threshold = [-10000, -22, -3, 11, 23, 10000]
    #         # threshold = [-23, -4, 0, 5.5, 5.7, 5.9, 10.9, 22.6, 24.2, 31]
    #         # threshold = [-23, -4, 6.3, 10.9, 28]
    #     elif col_name == 'slope':
    #         threshold = [-1000000, 0.0, 0.906551360842, 1.06534147176, 276.786553515, 726.000469512, 1000000]
    # elif sys.argv[2] == 'android':
    #     if col_name == "boottime":
    #         threshold = [-10000, -22, -2, -1, 1, 11, 10000]
    #     elif col_name == "slope":
    #         threshold = [-1000000, 0.0, 0.0953086226746, 0.164564280458, 2745.80703178, 1000000]
    global cat_threshold
    threshold = cat_threshold
    labels = [col_name + " {0}~{1}".format(threshold[i], threshold[i + 1]) for i in np.arange(0, len(threshold) - 1)]
    x[col_name + "_cat"] = pd.cut(x[col_name], threshold, right=True, labels=labels)
    return x



