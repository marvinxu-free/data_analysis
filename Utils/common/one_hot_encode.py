# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from sklearn.preprocessing import OneHotEncoder
import pickle
import os
import numpy as np


def get_one_hot_encode(df, cols, one_hot_len_dict, one_hot_pickle):
    """
    this function use FeatureHasher
    :param df:
    :param cols:
    :param one_hot_len_dict:
    :param one_hot_pickle:
    :return:
    """
    les_data = []
    if os.path.exists(one_hot_pickle):
        df[cols] = df[cols].astype(str)
        print('load one-hot encoder model')
        with open(one_hot_pickle, 'rb') as f:
            les = pickle.load(f)
        for col in cols:
            le = les[col]
            value = df[col].values.reshape(-1, 1)
            tr_data = le.transform(value)
            les_data.append(tr_data.A)
    else:
        print('save one-hot encoder model')
        les = {}
        df[cols] = df[cols].astype(str)
        for col in cols:
            print(col)
            n = one_hot_len_dict[col]
            le = OneHotEncoder(n_values=n)
            value = df[col].values.reshape(-1, 1)
            le.fit(value)
            les[col] = le
            tr_data = le.transform(value)
            les_data.append(tr_data.A)
        with open(one_hot_pickle, 'wb') as f:
            pickle.dump(les, f, -1)
    concate_data = np.concatenate(les_data, axis=1)
    return concate_data
