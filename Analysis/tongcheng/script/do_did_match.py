import sys

import numpy as np
from preprocess import did_modify

from Utils.tongcheng.prec_rec import do_online_prec_rec


def do_did_match(df, change_ratio):
    df = did_match(df.rename(columns={"prob": "dfp_prob"}), change_ratio)
    # print df.head()
    if "prob" in df.columns:
        df["prob"] = df[["prob", "dfp_prob"]].max(axis=1)
    else:
        df["prob"] = df["dfp_prob"]
    return df
    """
originalPair:  39789
predPair:  32213
rightPair:  32213
totalPair:  6729787
precision:  1.0 [1.0, 1.0]
recall:  0.809595616879 [0.80452134251336749, 0.81466989124470635]
    """


def did_match(df, change_ratio):
    """
    This function is to do did(imei, mac, aid for android and idfa, idfv for ios) match.
    Args:
        df:

    Returns:

    """
    if sys.argv[2] == 'android':
        df["imei_equals"] = (df["query_imei"] == df["doc_imei"]) + 0
        # df["mac_equals"] = (df["query_mac"] == df["doc_mac"]) + 0
        df["mac_equals"] = df.apply(lambda x: (x.query_mac == x.doc_mac) + 0 if (x.query_mac != '02:00:00:00:00:00') & (
            x.doc_mac != '02:00:00:00:00:00') else 0, axis=1)
        df["aid_equals"] = (df["query_aid"] == df["doc_aid"]) + 0
        df["did_match_label"] = map(lambda x1, x2, x3: 1 if x1 + x2 + x3 >= 1 else 0, df.imei_equals,
                                    df.mac_equals, df.aid_equals)
    elif sys.argv[2] == 'ios':
        df["idfa_equals"] = (df["query_idfa"] == df["doc_idfa"]) + 0
        df["idfv_equals"] = (df["query_idfv"] == df["doc_idfv"]) + 0
        df["did_match_label"] = map(lambda x1, x2: 1 if x1 + x2 >= 1 else 0, df.idfa_equals, df.idfv_equals)
    else:  # modify a part of did to be not matched
        data = df.to_dict(orient="records")
        eventID = Set()
        for i in data:
            eventID = eventID.union([i["doc_event_id"], i["query_event_id"]])
        event_changed = {}
        rng = np.random.RandomState(1000)
        rnd_num = rng.uniform(0, 1, len(eventID))
        k = 0
        print "event_num:", len(eventID)
        for i in eventID:
            random_number = rnd_num[k]
            if random_number >= change_ratio:
                event_changed[i] = 1
            else:
                event_changed[i] = 0
            k += 1
        df["did_match_label"] = df.apply(lambda x: did_modify(x, event_changed), axis=1)
    print "did_match done"
    df["prob_tmp"] = map(lambda x: 1.0 if x == 1 else 0.0, df.did_match_label)
    df["pred_tmp"] = map(lambda x: 1 if x == 1 else 0, df.did_match_label)
    if "prob" in df.columns:
        df["prob"] = df[["prob_tmp", "prob"]].max(axis=1)
        df["pred"] = df[["pred_tmp", "pred"]].max(axis=1)
    else:
        df["prob"] = df["prob_tmp"]
        df["pred"] = df["pred_tmp"]
    # df["ts_diff"] = map(lambda x: -1e9 if x == 1 else 1e9, df.did_match_label)
    print "do prec and rec:"
    do_online_prec_rec(df)
    return df