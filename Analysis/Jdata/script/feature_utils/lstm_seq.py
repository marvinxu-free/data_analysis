# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/5
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this function used to transoform datafram to lstm seq for next model train
"""
from __future__ import division, print_function
from pandas import DataFrame, concat
import pandas as pd
from pandas import Grouper


def lstm_seq(df, gcol, label='label', lstm_in=1, lstm_out=1):
    """
    this funtion used to
    :param df:
    :param label:
    :param lstm_in:
    :param lstm_out:
    :return:
    """
    print('lstm in {0}, out is {1}'.format(lstm_in, lstm_out))
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df = df.set_index('time'). \
        sort_index()
    data = []
    basic_cols = df.columns.difference(gcol)
    ## get input seq
    for i in range(1, lstm_in + 1):
        df1 = df.groupby(gcol)[basic_cols].shift(i)
        # df1 = df1.reset_index(0).sort_index().drop(gcol, axis=1)
        rename_cols = {x: "{0}(t-{1})".format(x, i) for x in basic_cols}
        df1 = df1.rename(columns=rename_cols)
        data.append(df1)
    ## get output seq
    for j in range(0, lstm_out):
        if j == 0:
            df2 = df.drop(gcol, axis=1)
        else:
            df2 = df.groupby(gcol)[basic_cols].shift(-j)
            # df2 = df2.reset_index(0).sort_index().drop(gcol, axis=1)
            rename_cols = {x: "{0}(t+{1})".format(x, j) for x in basic_cols}
            df2 = df2.rename(columns=rename_cols)
        data.append(df2)
    df_new = pd.concat(data, axis=1)
    print('lstm concate data shape is {0}'.format(df_new.shape))
    df_l = df_new.dropna()
    print('lstm data shape is {0}'.format(df_l.shape))
    last_month = df_l.index.month.values[-1]
    print('last month is {0}'.format(last_month))
    df_train = df_l.loc[df_l.index.month != last_month]
    df_test = df_l.loc[df_l.index.month == last_month]
    return df_train, df_test


