# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import division, print_function
import pandas as pd


def get_time_cat(x):
    """
    get day of month
    :param x:
    :return:
    """
    day = x.day
    weekth = (day - 1) // 7 + 1
    dayofweek = x.dayofweek
    hour = x.hour
    return [weekth, dayofweek, day, hour]


def create_new_col(df):
    """
    this function used to create new label for df
    :param df:
    :return:
    """
    df1 = df.set_index('login_time')
    g2 = ['id']
    dg = df1[['id', 'is_risk']].groupby(g2)

    def get_label(x):
        if x.any() == 1:
            return 1
        else:
            return 0

    df_tr = dg.transform(lambda x: get_label(x))
    df_tr = df_tr.rename(columns={'is_risk': 'label'})
    df_z = pd.concat([df1, df_tr], axis=1).drop(["is_risk", 'id', 'log_id'], axis=1)
    df_z = df_z.reset_index()
    df_z['weekth'], df_z['dayofweek'], df_z['day'], df_z['hour'] = zip(
        *df_z['login_time'].apply(lambda x: get_time_cat(x)))
    df_z = df_z.drop('login_time', axis=1)
    return df_z
