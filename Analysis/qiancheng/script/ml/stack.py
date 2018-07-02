# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com

"""
model:
    this file is used to stack multi machine learning model check if can improve precision
data:
    1. use 7:3
    2. change dataset from event to device
"""
from __future__ import print_function, division

import warnings
from datetime import timedelta

import numpy as np
import pandas as pd
from Utils.MultiColumnLabelEncoder import MultiColumnLabelEncoder

from Algorithm.qiancheng_stack_algorithm import stack_algorithm
from Utils.common.transfer_event_dev import transfer_event_to_device

warnings.filterwarnings("ignore")

def merge_ua_proxy(row):
    if row['uaMismatch.value'] == 'true' or row['proxyIP.value'] == 'true':
        return 1
    elif row['uaMismatch.value'] == True or row['proxyIP.value'] == True:
        return 1
    elif row['uaMismatch.value'] == 'True' or row['proxyIP.value'] == "True":
        return 1
    else:
        return 0

def read_data(file):
    df = pd.read_csv(file)
    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df["timestamp"] = pd.DatetimeIndex(df["timestamp"]) + timedelta(hours=8)
    df = df.sort_values(by="timestamp")
    # df = df.drop(["timestamp", "timestamp", 'ipSeg24', 'ipGeo', 'event_id', 'scenario'], axis=1)
    df['proxy_ua'] = df.apply(merge_ua_proxy, axis=1)
    df = df.drop(['uaMismatch.value', 'proxyIP.value',\
                  'proxyIP.anomaly', "timestamp", 'ipSeg24', 'ipGeo', 'scenario'],axis=1)
    df[['label']] = df[['label']].fillna(0)
    df[['label']] = df[['label']].astype(int)
    df = df.loc[df['event_type'] == 'ACT']
    df = df.drop(['event_type'], axis=1)
    df = transfer_event_to_device(df=df)
    return df

def ios_main(df,os='ios'):

    df = df.loc[df.os == os]

    bool_cols = df.select_dtypes(include=[np.bool_]).columns.tolist()
    obj_cols = df.select_dtypes(include=[np.object_]).columns.tolist()
    encoder_cols = bool_cols + obj_cols
    if 'maxent_id' in encoder_cols:
        encoder_cols.remove('maxent_id')

    df = MultiColumnLabelEncoder(columns=encoder_cols).fit_transform(df)

    stack_algorithm(df=df)

if __name__ == "__main__":
    file = "/Users/chaoxu/code/local-spark/Data/qiancheng_data/qiancheng_sample_new_merge_0.09/data.csv"
    df = read_data(file=file)
    ios_main(df=df)
