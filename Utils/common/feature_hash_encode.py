# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from sklearn.feature_extraction import FeatureHasher
import pickle
import os
import numpy as np


def get_feature_hash_encode(df, cols, hash_len_dict, hash_pickle):
    """
    this function use FeatureHasher
    :param df:
    :param cols:
    :param hash_len_dict:
    :param hash_pickle:
    :return:
    """
    les_data = []
    if os.path.exists(hash_pickle):
        print('load hash encoder model')
        df[cols] = df[cols].astype(str)
        with open(hash_pickle, 'rb') as f:
            les = pickle.load(f)
        for col in cols:
            le = les[col]
            tr_data = le.transform(df[col])
            les_data.append(tr_data.A)
    else:
        print('save hash encoder model')
        les = {}
        df[cols] = df[cols].astype(str)
        for col in cols:
            print(col)
            n = hash_len_dict[col]
            le = FeatureHasher(n_features=n, input_type='string', non_negative=True)
            le.fit(df[col])
            les[col] = le
            tr_data = le.transform(df[col])
            les_data.append(tr_data.A)
        with open(hash_pickle, 'wb') as f:
            pickle.dump(les, f, -1)
    concate_data = np.concatenate(les_data, axis=1)
    return concate_data
