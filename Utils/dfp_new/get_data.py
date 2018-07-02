# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/10
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to read and parse data for dfp detail
start from shanyin
"""
from Utils.common.custerReadFile import custom_open
import re
import datetime
import pandas as pd
import json
from sklearn.preprocessing import MinMaxScaler
import numpy as np
entropy_match = re.compile('^.*_entropy$')
from Utils.common.custerReadFile import read_files


def readData(path, os_sys="ios"):
    """
    read data from dfp details, use custom read function
    :param file_:
    :param os_sys:
    :return:
    """
    data = read_files(path=path)
    res = []
    for doc in data:
        doc = json.loads(doc)
        a = {}
        diff = datetime.datetime.fromtimestamp(doc['sourceDoc']['timestamp'] / 1000) \
               - datetime.datetime.fromtimestamp(doc['query']['timestamp'] / 1000)
        a['event_fly'] = abs(diff.total_seconds() / 60)
        a['resolution'] = doc["query"].get("resolution", "UNKNOWN")
        for (x, y ) in doc["dfpDetail"].items():
            entropy_flg = entropy_match.match(x)
            if entropy_flg:
                key = y['addEntropy']['level'] + "_entropy"
                a[key] = float(y['entropy'])
            else:
                if x == "constants" or x == 'resolution':
                    pass
                elif x == "client_ts":
                    key = y['addEntropy']['level'] + "_entropy"
                    a[key] = float(y['entropy'])
                else:
                    a[x] = y
        if doc.get('label_expect') is not None:
            flg = doc.get('label_expect') == "match"
            if flg:
                a['label'] = 1
            else:
                a['label'] = 0
        elif doc.get('expect') is not None:
            flg = doc.get('expect') == "match"
            if flg:
                a['label'] = 1
            else:
                a['label'] = 0
        else:
            a['label'] = 0
        a['score'] = doc.get('score',0)
        res.append(a)
    df = pd.DataFrame(res)
    df[['slope']] = df[['slope']].astype(float)
    df[['label']] = df[['label']].fillna(0)

    # obj_cols = df.select_dtypes(include=[np.object_]).columns.tolist()
    # for col in obj_cols:
    #     dummy_col = pd.get_dummies(df[col])
    #     df = pd.concat([df, dummy_col], axis=1)
    #
    # df.drop(obj_cols, inplace=True, axis=1)
    df_resolution = pd.get_dummies(df.resolution)
    df = pd.concat([df, df_resolution], axis=1)

    ts_match = re.compile('^.*ts_diff$')
    ts_amatch = np.vectorize(lambda x: bool(ts_match.match(x)))
    ts_cols = df.columns.values[ts_amatch(df.columns.values)]
    ts_df = df[ts_cols]
    ts_no = df[df.columns.difference(ts_cols)]

    min_max_scaler = MinMaxScaler()
    X_scaled = min_max_scaler.fit_transform(ts_df)
    ts_df = pd.DataFrame(X_scaled, columns=ts_df.columns)
    df = pd.concat([ts_no, ts_df], axis=1)
    return df

