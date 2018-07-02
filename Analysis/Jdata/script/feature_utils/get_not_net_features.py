# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import  division, print_function
import pandas as pd
import numpy as np


def get_not_net_features(df, col='id', time_range='7D', feature_cols=[]):
    """
    获取非网络特征
    :param df:
    :param time_range:
    :param feature_cols:
    :return:
    """

    def get_max_count(x):
        (values, counts) = np.unique(x, return_counts=True)
        ind = np.argmax(counts)
        return values[ind]

    df[feature_cols] = df[feature_cols].astype(int)
    df_g = df.groupby(col)[feature_cols].rolling(time_range).apply(lambda x: get_max_count(x))
    df_g = df_g.reset_index(0).sort_index().drop(col, axis=1)
    rename_cols = {x: "new_anomaly_{0}_{1}".format(x, time_range) for x in feature_cols}
    df_g = df_g.rename(columns=rename_cols)
    df_g = pd.concat([df[feature_cols], df_g], axis=1)
    for col1 in feature_cols:
        col_u = rename_cols[col1]
        df_g[col_u] = df_g.apply(lambda x: 1 if x[col_u] == x[col1] else 0, axis=1)
    df_g = df_g[rename_cols.values()]
    return df_g, rename_cols
