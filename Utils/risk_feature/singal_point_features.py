# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/13
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本文件主要用于获取单点特征对应的数据
"""
from __future__ import division, print_function
import pandas as pd


def get_cluster_data(df, fcol, gcol, acol, path=None):
    """
    根据fcol过滤异常df， 这里fcol对应的是bool column
    根据gcol聚合
    nunique acol
    :param df:
    :param fcol:
    :param gcol:
    :param acol:
    :return:
    """
    all_acol_name = "value"
    df = df.copy()
    df1 = df.groupby(gcol).agg({acol:"nunique"}).reset_index()
    df1 = df1.rename(columns={acol:all_acol_name})
    df2 = df.loc[df[fcol] == True]
    df2 = df2.groupby(gcol).agg({acol:"nunique", "timestamp":"min"}).reset_index()
    df = df1.merge(df2, on=gcol)
    df['ratio'] = df[acol] / df[all_acol_name]
    df = df.loc[(df[acol] > df[acol].describe().ix[4]) & (df['ratio'] > 0.05 )]
    df = df.sort_values(['ratio', acol, "timestamp"], ascending=[False, True, True])
    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.iloc[:20]
    if path:
        path1 = path + "/{0}_clutser_in_{1}_value.json".format(acol,gcol)
        path2 = path + "/{0}_clutser_in_{1}_ratio.json".format(acol,gcol)
        df[[gcol,'value']].to_json(path1, force_ascii=False, orient='records', lines=True)
        df[[gcol,'ratio']].to_json(path2, force_ascii=False, orient='records', lines=True)
    return df