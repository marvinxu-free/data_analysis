# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function,division
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Pic.maxent_font import tick_font

def transfer_event_to_device(df,mode=0):
    bool_df = df.select_dtypes(include=['bool'])
    bool_cols =bool_df.columns

    obj_df = df.select_dtypes(include=['object'])
    obj_cols = obj_df.columns

    anormaly = re.compile('.*anomaly$')
    anormaly_match = np.vectorize(lambda x: bool(anormaly.match(x)))
    anormaly_cols = df.columns.values[anormaly_match(df.columns.values)]

    value = re.compile('.*value$')
    value_match = np.vectorize(lambda x: bool(value.match(x)))
    value_cols = df.columns.values[value_match(df.columns.values)]
    value_cols = list(set(value_cols) - set(bool_cols))

    count = re.compile('.*counts$')
    count_match = np.vectorize(lambda x: bool(count.match(x)))
    count_cols = df.columns.values[count_match(df.columns.values)]

    loan = re.compile('.*loan$')
    loan_match = np.vectorize(lambda x: bool(loan.match(x)))
    loan_cols = df.columns.values[loan_match(df.columns.values)]
    # df = df.drop(bool_cols,axis=1)
    if df['cracked.value'].dtype == np.object_:
        df['cracked.value'] = df['cracked.value'].astype(bool)
    if df['idcIP.value'].dtype == np.object_:
        df['idcIP.value'] = df['idcIP.value'].astype(bool)

    df[anormaly_cols] = df[anormaly_cols].fillna(1)
    df[value_cols] = df[value_cols].fillna(1)
    df[loan_cols] = df[loan_cols].fillna(0)
    df[count_cols] = df[count_cols].fillna(0)
    f = {"label":'max',
         "proxy_ua":'max'}
    for col in anormaly_cols:
        f[col] = 'max'
    for col in loan_cols:
        f[col] = 'max'
    for col in value_cols:
        f[col] = 'mean'
    for col in count_cols:
        f[col] = 'mean'
    for col in bool_cols:
        f[col] = 'max'
    if mode == 0:
        df = df.groupby(['maxent_id','os'],as_index=False).agg(f)
        return df
    elif mode == 1:
        df_event_num  = df.groupby(['maxent_id','os'],as_index=False)['event_id'].agg({"event_num":'count'})
        df_event_num = df_event_num.drop(['maxent_id','os'],axis=1)
        df = df.groupby(['maxent_id','os'],as_index=False).agg({'label':'max'})
        df = pd.concat([df_event_num,df],axis=1)
        return df
    else:
        df_event_num  = df.groupby(['maxent_id','os'],as_index=False)['event_id'].agg({"event_num":'count'})
        df_event_num = df_event_num.drop(['maxent_id','os'],axis=1)
        df = df.groupby(['maxent_id','os'],as_index=False).agg(f)
        df = pd.concat([df_event_num,df],axis=1)
        return df
