import json
import sys
import uuid
import os
import pandas as pd
import re


def read_online_data(fname):
    with open(fname, "r") as f:
        data = f.readlines()
    res = []
    for doc in data:
        doc = json.loads(doc)
        res.append(doc)
    df = pd.DataFrame(res)
    df.rename(columns={"score": "prob", "query_id": "query_event_id", "doc_id": "doc_event_id",
                       "label_match": "label", "query_device": "query_brand", "doc_device": "doc_brand"},
              inplace=True)
    # print df.head()
    print len(df)
    return df


def desc_data(df):
    print "dataframe size:\t", len(df)
    print "positive sample size:\t", len(df[df.label == 1])
    print df["ua"].value_counts()
    print df["ip"].value_counts()
    print df["boottime"].min()
    print df["boottime"].max()
    print df["boottime_cat"].value_counts()


def read_dfp_details(fname):
    entropy_match = re.compile('^.*_entropy$')
    file_list = os.listdir(fname)
    data_set = []
    for file in file_list[1:]:
        filepath = fname + file
        with open(filepath, "r") as f:
            data = f.readlines()
        data_set = data_set + data
    res = []
    for doc in data_set:
        doc = json.loads(doc)
        a ={}
        source_doc = doc["sourceDoc"]
        query_doc = doc["query"]
        doc["source_platform"] = 1 if source_doc.get("inApp") else 0
        doc["query_platform"] = 1 if query_doc.get("inApp") else 0
        doc["source_label"] = source_doc.get("did").get("label").lower()
        doc["query_label"] = query_doc.get("did").get("label").lower()
        a["source_tcpts"] = source_doc.get("tcpts")
        a["query_tcpts"] = query_doc.get("tcpts")
        a["label"] = 1 if doc["query_label"] == doc["source_label"] else 0
        for (x, y) in doc["dfpDetail"].items():
            entropy_flg = entropy_match.match(x)
            if entropy_flg:
                key = y['addEntropy']['level'] + "_entropy"
                a[key] = float(y['entropy'])
            else:
                if x == "constants":
                    pass
                elif x == "client_ts":
                    key = y['addEntropy']['level'] + "_entropy"
                    a[key] = float(y['entropy'])
                else:
                    a[x] = y
        a["source_ts"] = source_doc["timestamp"]
        a["query_ts"] = query_doc["timestamp"]
        a["ts_diff"] = (a["query_ts"] - a["source_ts"])/1000.0
        a["slope"] = (a["query_tcpts"] - a["source_tcpts"]) / a["ts_diff"]
        a["match_type"] = doc["source_platform"] + doc["query_platform"]
        if a['match_type'] == 2:
            query_mcid = query_doc['did'].get('mcid', 'qmcid')
            source_doc_mcid = source_doc['did'].get('mcid', 'smcid')
            a['did_label'] = 1 if query_mcid == source_doc_mcid else 0
        elif a['match_type'] == 0:
            query_ckid = query_doc['did'].get('ckid', 'qckid')
            source_doc_ckid = source_doc['did'].get('ckid','sckid')
            a['did_label'] = 1 if query_ckid == source_doc_ckid else 0
        else:
            a['did_label'] = 0
        res.append(a)
    df = pd.DataFrame(res)
    return df

def read_data(fname):
    with open(fname, "r") as f:
        data = f.readlines()
    res = []
    for doc in data:
        doc = json.loads(doc)
        doc_did = doc["doc_did"]
        query_did = doc["query_did"]
        if sys.argv[2] == 'android':
            doc_imei = doc_did.get("imei", str(uuid.uuid4()))
            doc_mac = doc_did.get("mac", str(uuid.uuid4()))
            doc_aid = doc_did.get("aid", str(uuid.uuid4()))
            doc["doc_imei"] = doc_imei
            doc["doc_mac"] = doc_mac
            doc["doc_aid"] = doc_aid
            query_imei = query_did.get("imei", str(uuid.uuid4()))
            query_mac = query_did.get("mac", str(uuid.uuid4()))
            query_aid = query_did.get("aid", str(uuid.uuid4()))
            doc["query_imei"] = query_imei
            doc["query_mac"] = query_mac
            doc["query_aid"] = query_aid
        elif sys.argv[2] == 'ios':
            doc_idfa = doc_did.get("idfa", str(uuid.uuid4()))
            doc_idfv = doc_did.get("idfv", str(uuid.uuid4()))
            doc["doc_idfa"] = doc_idfa
            doc["doc_idfv"] = doc_idfv
            query_idfa = query_did.get("idfa", str(uuid.uuid4()))
            query_idfv = query_did.get("idfv", str(uuid.uuid4()))
            doc["query_idfa"] = query_idfa
            doc["query_idfv"] = query_idfv
        doc["doc_mcid"] = doc_did.get("mcid", str(uuid.uuid4()))
        doc["doc_ckid"] = doc_did.get("ckid", str(uuid.uuid4()))
        doc["query_mcid"] = query_did.get("mcid", str(uuid.uuid4()))
        doc["query_ckid"] = query_did.get("ckid", str(uuid.uuid4()))
        res.append(doc)
    # data = map(lambda x: json.loads(x.rstrip()), data)
    df = pd.DataFrame(res)
    return df
