# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/6
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本文件使用之前的prepare_feature_lstm.py产生的特征数据，
使用LSTM算法训练模型并评估结果
"""
from __future__ import division, print_function
from Params.jd_params import *
import pandas as pd
import numpy as np
import os
import json
import pickle
from sklearn.preprocessing import MinMaxScaler
from feature_utils.lstm_model import lstm_model
# from feature_utils.xgb_model import jd_xgb_c


def get_data():
    df_train = pd.read_csv(lstm_train_data, index_col=[0], parse_dates=[0])
    df_train = df_train.astype(float)
    df_test = pd.read_csv(lstm_test_data, index_col=[0], parse_dates=[0])
    df_test = df_test.astype(float)
    # X_cols = df_train.columns.difference(['label'])
    label_cols = df_train.columns.values[label_match(df_train.columns.values)].tolist()
    X_cols = df_train.columns.difference(label_cols)
    X_train = df_train[X_cols]
    y_train = df_train['label'].values
    X_test = df_test[X_cols]
    y_test = df_test['label'].values
    return X_train, y_train, X_test, y_test


def model_train(post_fix='1v'):
    X_train, y_train, X_test, y_test = get_data()
    scaler = MinMaxScaler(feature_range=(0, 1))
    X_train = scaler.fit_transform(X_train)
    print('save feature scale model for predict to {0}'.format(lstm_feature_scale_pkl))
    with open(lstm_feature_scale_pkl, 'wb') as f:
        pickle.dump(scaler, f, -1)

    X_test = scaler.transform(X_test)
    X_train = X_train.reshape((X_train.shape[0], 1, X_train.shape[1]))
    X_test = X_test.reshape((X_test.shape[0], 1, X_test.shape[1]))
    print(X_train.shape, y_train.shape, X_test.shape, y_test.shape)
    # y_train = np_utils.to_categorical(y_train, 2)
    # y_test = np_utils.to_categorical(y_test, 2)
    lstm_model(X_train=X_train, X_test=X_test, y_train=y_train, y_test=y_test, post_fix=post_fix)
    # jd_xgb_c(X_train=X_train, X_test=X_test, y_train=y_train, y_test=y_test, post_fix=post_fix)


if __name__ == '__main__':
    # sess = K.get_session()
    # sess = tf_debug.LocalCLIDebugWrapperSession(sess)
    # K.set_session(sess)
    model_train()
