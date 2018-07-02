# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/2
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import division, print_function
from feature_utils.get_features_trade import prepare_features as trade_features
from Params.jd_params import *
import os
import errno
from feature_utils.create_new_col import get_time_cat
from feature_utils.feature_encode import feature_encode
from Pic.hist import pic_label
import re
import numpy as np
import json


def get_features():
    df_train, df_test = trade_features(f1=train_login, f2=train_trade)
    if not os.path.exists(v3_img_path):
        try:
            os.makedirs(v3_img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    # df_train.to_csv(v3_train_data, index=False)
    # df_test.to_csv(v3_test_data, index=False)
    pic_label(df=df_train, col='label', title=u"训练数据风险比例", file_path=v3_img_path)
    pic_label(df=df_test, col='label', title=u"验证数据风险比例", file_path=v3_img_path)
    df_train = df_train.reset_index() \
        .sort_values(by=['login_time', 'time'], ascending=[True, True]) \
        .drop('login_time', axis=1)
    df_test = df_test.reset_index() \
        .sort_values(by=['login_time', 'time'], ascending=[True, True]) \
        .drop('login_time', axis=1)

    with open(holiday_data, 'r') as f:
        holidays = json.load(f)

    def check_hoilday(x):
        day = str(x.day)
        holiday_chk = holidays.get(day, 0)
        if holiday_chk == 1 or holiday_chk == 2:
            return 1
        else:
            return 0

    df_train['new_is_holiday'] = df_train['time'].apply(lambda x: check_hoilday(x))
    df_test['new_is_holiday'] = df_test['time'].apply(lambda x: check_hoilday(x))

    new_cols = df_train.columns.values[new_match(df_train.columns.values)]

    df_train['weekth'], df_train['dayofweek'], df_train['day'], df_train['hour'] = zip(
        *df_train['time'].apply(lambda x: get_time_cat(x)))
    df_train = df_train.drop('time', axis=1)

    df_test['weekth'], df_test['dayofweek'], df_test['day'], df_test['hour'] = zip(
        *df_test['time'].apply(lambda x: get_time_cat(x)))
    df_test = df_test.drop('time', axis=1)

    cols_need_enc = df_train.columns.difference(new_cols)
    print('will encode these columns {0}'.format(cols_need_enc))
    train_enc_data = feature_encode(df_train[cols_need_enc],
                                    one_hot_pkl_file=v3_one_hot_pkl, hashe_pkl_file=v3_feature_hash_pkl)
    train_npy = np.concatenate([df_train[new_cols].values, train_enc_data], axis=1)
    print('train data shape:{0}'.format(train_npy.shape))
    np.save(v3_train_data, train_npy)

    test_enc_data = feature_encode(df_test[cols_need_enc],
                                   one_hot_pkl_file=v3_one_hot_pkl, hashe_pkl_file=v3_feature_hash_pkl)
    test_npy = np.concatenate([df_test[new_cols].values, test_enc_data], axis=1)
    print('test data shape:{0}'.format(test_npy.shape))
    np.save(v3_test_data, test_npy)


if __name__ == '__main__':
    get_features()
