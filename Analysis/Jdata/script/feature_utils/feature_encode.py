# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import division, print_function
from Params.jd_params import *
from Utils.common.one_hot_encode import get_one_hot_encode
from Utils.common.feature_hash_encode import get_feature_hash_encode
import numpy as np


def feature_encode(df,  one_hot_pkl_file, hashe_pkl_file):
    """
    本函数用于对分类特征进行编码， 返回numpy数据
    :param df:
    :return:
    """
    one_hot_cols = one_hot_dict.keys()
    feature_hasher_cols = feature_hasher_dict.keys()
    not_touch_X_cols = df.columns.difference(one_hot_cols).difference(feature_hasher_cols).difference(['label'])
    one_hot_data = get_one_hot_encode(df=df, cols=one_hot_cols, one_hot_len_dict=one_hot_dict,
                                      one_hot_pickle=one_hot_pkl_file)
    feature_hasher_data = get_feature_hash_encode(df=df, cols=feature_hasher_cols, hash_len_dict=feature_hasher_dict,
                                                  hash_pickle=hashe_pkl_file)
    X_not_touch = df[not_touch_X_cols].values
    y = df[['label']].values
    data = np.concatenate([one_hot_data, feature_hasher_data, X_not_touch, y], axis=1)
    return data

