# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import division,print_function
import numpy as np


def get_login_features(df, col='id', time_range='7D', feature_cols=[]):
    """
    this function usde to get login features
    :param df:
    :param col:
    :param feature_cols:
    :param time_range:
    :return:
    """
    df_g = df.groupby(col)[feature_cols].rolling(time_range).apply(lambda x: np.unique(x).shape[0])
    df_g = df_g.reset_index(0).sort_index().drop(col, axis=1)
    rename_cols = {x: "new_{0}_login_{1}_{2}_num".format(col, x, time_range) for x in feature_cols}
    df_g = df_g.rename(columns=rename_cols)
    return df_g, rename_cols
