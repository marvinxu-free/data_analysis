# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/23
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to prepare lstm feature functions
"""
from __future__ import print_function, division
from Params.jd_params import *
import pandas as pd
import numpy as np
import os
import json
from get_login_features import get_login_features
from create_new_col import get_time_cat
from lstm_seq import *


def get_nunique(df, gcol, agg_cols, time_unit):
    g = Grouper(key=gcol, freq=time_unit)
    agg_dict = {x: 'nunique' for x in agg_cols}
    df_new = df.groupby(g).agg(agg_dict)
    df_new = df_new.reset_index(0).sort_index().drop(gcol, axis=1)
    return df_new


def get_mean(df, gcol, agg_cols, time_unit):
    # g = Grouper(key=gcol, freq=time_unit)
    # agg_dict = {x: 'mean' for x in agg_cols}
    # df_new = df.groupby(g).agg(agg_dict)
    df_new = df.groupby(gcol)[agg_cols].rolling(time_unit).mean()
    df_new = df_new.reset_index(0).sort_index().drop(gcol, axis=1)
    new_cols = {x: 'new_{0}_{1}_mean'.format(x, time_unit) for x in agg_cols}
    df_new = df_new.rename(columns=new_cols)
    return df_new, new_cols


def chk_exist(df, gcol, time_unit, *chk_rol):
    g = Grouper(key=gcol, freq=time_unit)
    df_new = df.groupby(g)[chk_rol].applymap(lambda x: 1 if np.where(x == 1).any() else 0)
    return df_new


def get_feature_lstm(f1, f2, time_unit='30Min', lstm_in=1, lstm_out=1):
    """

    :param f1:
    :param f2:
    :return:
    """
    if os.path.exists("{0}/lstm_feature_debug.csv".format(data_path)):
        df_z = pd.read_csv("{0}/lstm_feature_debug.csv".format(data_path), index_col=[0], parse_dates=[0])
    else:
        print(f1)
        print(f2)
        df_trade = pd.read_csv(f2)
        df_trade['time'] = pd.to_datetime(df_trade['time'], utc=True)
        df_login = pd.read_csv(f1)
        df_login[['is_scan', 'is_sec']] = df_login[['is_scan', 'is_sec']].astype(int)
        df_login = df_login.drop('timestamp', axis=1)
        df_login['time'] = pd.to_datetime(df_login['time'], utc=True)
        df_login = df_login.rename(columns={'time': 'login_time'})

        df = df_trade.merge(df_login, on='id', how='left')
        df = df.loc[df.login_time < df.time]
        df = df.sort_values(['login_time', 'time'], ascending=[True, True])

        # df_login_trade = df.groupby('rowkey').last().reset_index()
        #
        # df = df_login.merge(df_login_trade[['login_time', 'is_risk', 'id', 'rowkey', 'time']], on=['id', 'login_time'],
        #                     how='left')
        # df['time_delta'] = df.loc[df.time.notnull()].apply(lambda x: (x['time'] - x['login_time']), axis=1)
        df['time_delta'] = df.apply(lambda x: (x['time'] - x['login_time']), axis=1)
        df['time_delta'] = df['time_delta'].apply(lambda x: x.seconds)
        df = df.set_index('login_time')
        df = df.sort_index()
        print('sort index')
        # 对每个人隔30min计算一次平均登陆时间， 以及交易登陆时间差的平均
        df_time, _ = get_mean(df, 'id', agg_cols=['time_delta', 'timelong'], time_unit=time_unit)
        # 对每个人隔30min计算使用的不重复的ip, city, type, result, log_from, device数量
        login_features = ['city', 'type', 'result', 'log_from', 'device', 'ip']
        df_nunique, _ = \
            get_login_features(df=df, col='id', feature_cols=login_features, time_range=time_unit)

        assert df_time.shape[0] == df_nunique.shape[0] and df_time.shape[0] == df.shape[0]

        df_new = pd.concat([df, df_time, df_nunique], axis=1)
        print('concat shape is {0}'.format(df_new.shape))
        print('concat done')

        df_z = df_new.reset_index(). \
            sort_values(by=['login_time', 'time'], ascending=[True, True])
        # sort_values(by=['login_time', 'time'], ascending=[True, True]). \
        # groupby('rowkey'). \
        # last()
        df_z = df_z.rename(columns={'is_risk': 'label'})

        with open(holiday_data, 'r') as f:
            holidays = json.load(f)

        def check_hoilday(x):
            day = str(x.day)
            holiday_chk = holidays.get(day, 0)
            if holiday_chk == 1 or holiday_chk == 2:
                return 1
            else:
                return 0

        df_z['new_trade_is_holiday'] = df_z['time'].apply(lambda x: check_hoilday(x))
        df_z['new_trade_weekth'], df_z['new_trade_dayofweek'], df_z['new_trade_day'], df_z['new_trade_hour'] = zip(
            *df_z['time'].apply(lambda x: get_time_cat(x)))

        print(df_z.columns)
        print(df_z.shape)
        new_cols = df_z.columns.values[new_match(df_z.columns.values)].tolist()
        use_cols = new_cols + ['time', 'id', 'rowkey', 'is_scan', 'is_sec', 'label']
        print('actuall used cols is {0}'.format(use_cols))

        df_z = df_z[use_cols]
        df_z.to_csv("{0}/lstm_feature_debug.csv".format(data_path), index=False)
    df_train, df_test = lstm_seq(df=df_z,
                                 gcol=['id', 'rowkey', 'new_trade_weekth',
                                       'new_trade_dayofweek', 'new_trade_day', 'new_trade_hour'],
                                 lstm_in=lstm_in, lstm_out=lstm_out)
    return df_train, df_test
