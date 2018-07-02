# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/20
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function, division

import warnings
from datetime import timedelta

import numpy as np
import pandas as pd
from Utils.common.MultiColumnLabelEncoder import MultiColumnLabelEncoder

from Algorithm.qiancheng_tree import qiancheng_tree_rf
from Utils.common.transfer_event_dev import transfer_event_to_device
import re

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
                  "timestamp", 'ipSeg24', 'ipGeo', 'scenario'],axis=1)
    df[['label']] = df[['label']].fillna(0)
    df[['label']] = df[['label']].astype(int)
    df = df.loc[df['event_type'] == 'ACT']
    df = df.drop(['event_type'], axis=1)
    df = transfer_event_to_device(df=df)
    return df


def df_main(df,os='ios',postfix=0.09):
    df = df.loc[df.os == os]
    bool_cols = df.select_dtypes(include=[np.bool_]).columns.tolist()
    obj_cols = df.select_dtypes(include=[np.object_]).columns.tolist()
    encoder_cols = bool_cols + obj_cols
    if 'maxent_id' in encoder_cols:
        encoder_cols.remove('maxent_id')
    df = MultiColumnLabelEncoder(columns=encoder_cols).fit_transform(df)
    if os == 'ios':
        col_drop = ['os','maxent_id', 'aid_loan', 'imei_loan', 'mac_loan', 'imei_counts', 'mac_counts', 'aid_counts']
    else:
        col_drop = ['os','maxent_id', 'idfa_loan', 'idfa_counts', 'idfv_counts', 'imei_loan']

    df = df.drop(col_drop,axis=1)
    qiancheng_tree_rf(df=df,os_sys=os,postfix=postfix)


def read_dev_data(path):
    df = pd.read_csv(path)
    anormaly = re.compile('.*anomaly$')
    anormaly_match = np.vectorize(lambda x: bool(anormaly.match(x)))
    anormaly_cols = df.columns.values[anormaly_match(df.columns.values)]
    value = re.compile('.*value$')
    value_match = np.vectorize(lambda x: bool(value.match(x)))
    value_cols = df.columns.values[value_match(df.columns.values)]
    old_names = anormaly_cols.tolist() + value_cols.tolist()
    new_names = map(lambda x: x.replace("_", "."), old_names)
    df.rename(columns=dict(zip(old_names, new_names)), inplace=True)
    return df