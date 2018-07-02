# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/26
# Company : Maxent
# Email: chao.xu@maxent-inc.com

"""
this file used tensorflow dnn instead
of xgboost
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import numpy as np
import pandas as pd
from Utils.common.custerReadFile import custom_open
import json
import re
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
tf.logging.set_verbosity(tf.logging.INFO)

entropy_match = re.compile('^.*_entropy$')


def read_data(file_, os_sys="ios"):
    with custom_open(file_) as f:
        data = f
        res = []
        for doc in data:
            doc = json.loads(doc)
            a = {}
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


def get_input_fn(data_set,feature_cols,target,num_epochs=None,shuffle=True):
    """
    this function is used to get data from pandas dataframe
    :param data_set:
    :param feature_cols:
    :param target:
    :param num_epochs:
    :param shuffle:
    :return:
    """
    return tf.estimator.inputs.pandas_input_fn(
      x=pd.DataFrame({k: data_set[k].values for k in feature_cols}),
      y=pd.Series(data_set[target].values),
      num_epochs=num_epochs,
      shuffle=shuffle)

