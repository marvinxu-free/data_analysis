# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import print_function, division


def get_risk_history(df, col):
    """
    get user ever do risk trade before
    :param df:
    :param col:
    :return:
    """
    col_ = "new_happened_{0}".format(col)
    df_new = df.groupby('id')[[col]].expanding().apply(lambda x: 1 if x.any() == 1 else 0)
    df_new = df_new.rename(columns={col: col_})
    df_new = df_new.reset_index(0).sort_index().drop('id', axis=1).fillna(0)
    return df_new, col_
