# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import print_function, division
from Params.jd_params import *
import pandas as pd
from pandas import Grouper
import numpy as np
import os
import json
from get_not_net_features import get_not_net_features
from get_login_features import get_login_features
from get_risk_history import get_risk_history
from create_new_col import get_time_cat


def prepare_features(f1, f2):
    """
    :param f1: file for login data
    :param f2: file for trade data
    :return:
    """
    if os.path.exists("{0}/get_feature_debug.csv".format(data_path)):
        df_z = pd.read_csv("{0}/get_feature_debug.csv".format(data_path), index_col=[0], parse_dates=[0])
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
        df_login_trade = df.groupby('rowkey').last().reset_index()

        df = df_login.merge(df_login_trade[['login_time', 'is_risk', 'id', 'rowkey', 'time']], on=['id', 'login_time'],
                            how='left')
        df['new_time_delta'] = df.loc[df.time.notnull()].apply(lambda x: (x['time'] - x['login_time']), axis=1)
        df = df.set_index('login_time')
        df = df.sort_index()
        print('sort index')

        # 检查一天内是否有多次登录
        df_r = df.groupby('id')['log_id'].rolling('1D').apply(lambda x: np.unique(x).shape[0])
        df_r = df_r.reset_index(0).sort_index()
        df_r = df_r.rename(columns={'log_id': 'new_one_day_login'})
        df_r['new_one_day_login'] = df_r['new_one_day_login'].apply(lambda x: 1 if x > 1 else 0)
        df_r = df_r.drop('id', axis=1)

        # 检测历史是否有欺诈
        df_risk_h, risk_h_col = get_risk_history(df, col='is_risk')
        print('risk features done')
        # 网络特征
        login_features = ['city', 'type', 'result', 'log_from', 'device', 'ip']
        df_login_features, login_cols = get_login_features(df=df, col='id', feature_cols=login_features)
        print('network features done')
        # 非网络特征
        device_features = ['city', 'type', 'result', 'log_from', 'device', 'ip']
        df_dev_features, dev_cols = get_not_net_features(df=df, feature_cols=device_features)
        print('not network features done')

        df_new = pd.concat([df, df_r, df_login_features, df_dev_features, df_risk_h], axis=1)
        print('concat done')

        df_z = df_new.reset_index(). \
            sort_values(by=['login_time', 'time'], ascending=[True, True]). \
            groupby('rowkey'). \
            last(). \
            set_index('time'). \
            sort_index()
        df_z = df_z.rename(columns={'is_risk': 'label'})
        df_z['new_time_delta'] = df_z['new_time_delta'].apply(lambda x: x.seconds)
        print(df_z.columns)
        print(df_z.columns[df_z.isnull().any()])
        print("all features are: {0}".format(df_z.columns.tolist()))
        df_z.to_csv("{0}/get_feature_debug.csv".format(data_path))
    last_month = df_z.index.month.values[-1]
    print('last month is {0}'.format(last_month))
    df_train = df_z.loc[df_z.index.month != last_month]
    df_test = df_z.loc[df_z.index.month == last_month]
    return df_train, df_test







