# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com
#
"""
this file is used xgboost algorithm on shan yin dfp for ios system
"""
from __future__ import print_function,division
import numpy as np
import pandas as pd
import datetime
from Utils.common.custerReadFile import custom_open
from Algorithm.dfp_xgboost import ios_dfp_xg,ios_dfp_p2,ios_dfp_p4
import json
import re
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings("ignore")

entropy_match = re.compile('^.*_entropy$')

def readData(file_, os_sys="ios"):
    """
    read data from huanbei
    :param file_:
    :param os_sys:
    :return:
    """
    with custom_open(file_) as f:
        data = f
        res = []
        for doc in data:
            doc = json.loads(doc)
            a = {}
            diff = datetime.datetime.fromtimestamp(doc['sourceDoc']['timestamp'] / 1000)\
                              - datetime.datetime.fromtimestamp(doc['query']['timestamp'] / 1000)
            a['event_fly'] = abs(diff.total_seconds() / 60)
            if (doc['sourceDoc']['inApp'] and
                    doc['sourceDoc']['inApp'] == doc['query']['inApp']):
                a['dfp_type'] = 0
            elif (not doc['sourceDoc']['inApp'] and
                    doc['sourceDoc']['inApp'] == doc['query']['inApp']):
                a['dfp_type'] = 1
            else:
                a['dfp_type'] = 2

            for (x, y ) in doc["dfpDetail"].items():
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

        obj_cols = df.select_dtypes(include=[np.object_]).columns.tolist()
        for col in obj_cols:
            dummy_col = pd.get_dummies(df[col])
            df = pd.concat([df, dummy_col], axis=1)
        df.drop(obj_cols, inplace=True, axis=1)

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


def dfp_main(df, dfp_type, part='huanbei_app_app'):
    """
    dfp for app to app
    :param df:
    :param os:
    :param part:
    :return:
    """
    print("analysis {0}".format(part))
    df = df.loc[df['dfp_type'] == dfp_type]

    def show_missing():
        missing = df.columns[df.isnull().any()].tolist()
        return missing

    need_cols = df.columns.difference(
        ['baseEntropy', 'device_browser_engine_entropy',
        'device_osversion_entropy', 'province_entropy',
        'device_model', 'jsid_entropy', 'resolution', 'ts_diff',"dfy_type"],
    )

    df = df[need_cols]

    print("missing columns")
    df = df.fillna(-6.666)
    print(df[show_missing()].isnull().sum())
    print()

    ios_dfp_xg(df=df,postfix=part)

def main(file_):
    df = readData(file_=file_)
    dfp_main(df=df,dfp_type=0,part='huanbei_app_app')
    dfp_main(df=df,dfp_type=1,part='huanbei_app_web')
    # dfp_main(df=df,dfp_type=2,part='huanbei_web_web')

if __name__ == "__main__":
    # file_p1 = "/Users/chaoxu/code/local-spark/Data/ios_sample.json/data.json"

    file_p1 = "/Users/chaoxu/code/local-spark/Data/sample_huanbei/data.json"
    main(file_=file_p1)
