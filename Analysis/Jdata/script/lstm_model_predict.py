# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/9
# Company : Maxent
# Email: chao.xu@maxent-inc.com
""""
this function used to predict validation data,
used pre-trained model
"""
from __future__ import division, print_function
import keras
import pandas as pd
import numpy as np
import os
import json
import pickle
from Params.jd_params import *
from keras.models import model_from_json
from keras.utils import plot_model
from Utils.common.keras_udf_metric import pos_recall, pos_precision, pos_fmeasure
from Utils.common.keras_udf_loss import bin_categorical_crossentropy, wrapped_partial
from sklearn.metrics import classification_report
import errno
from Pic.confusion import plot_confusion_matrix_sns
from Pic.draw_roc import pic_roc_cmp
from Pic.pr_curve import pic_pr_cmp, pic_pr_cmp_threshold
from Pic.pic_fscore import pic_fbeta_cmp
import random as rn

np.random.seed(42)
rn.seed(12345)


def get_data():
    df_train = pd.read_csv(lstm_train_data, index_col=[0], parse_dates=[0])
    df_train = df_train.astype(float)
    df_test = pd.read_csv(lstm_test_data, index_col=[0], parse_dates=[0])
    df_test = df_test.astype(float)
    X_cols = df_train.columns.difference(['label'])
    X_train = df_train[X_cols]
    y_train = df_train[['label']].values
    X_test = df_test[X_cols]
    y_test = df_test[['label']].values
    return X_train, y_train, X_test, y_test


def model_predict(post_fix='1v'):
    X_train, y_train, X_test, y_test = get_data()
    udf_loss = wrapped_partial(bin_categorical_crossentropy, e1=1.2, e2=0.8)
    with open(lstm_feature_scale_pkl, 'rb') as f:
        scaler = pickle.load(f)
        X_test = scaler.transform(X_test)
    X_test = X_test.reshape((X_test.shape[0], 1, X_test.shape[1]))
    model_file = lstm_model_file.format(post_fix)
    model_hdf5 = lstm_hdf5_file.format(post_fix)
    with open(model_file, 'r') as f:
        model_str = json.load(f)
        model = model_from_json(model_str)
    model.load_weights(model_hdf5)
    y_pred_prob = model.predict(X_test)
    y_pred = np.round(y_pred_prob)
    print(classification_report(y_test, y_pred, target_names=['0', '1']))
    print("image path {0}".format(lstm_img_path))
    if not os.path.exists(lstm_img_path):
        try:
            os.makedirs(lstm_img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    ## plot model arch
    plot_model(model, to_file="{0}/lstm.png".format(lstm_img_path))
    ## plot confusion matrix
    plot_confusion_matrix_sns(y_pred=y_pred, y_test=y_test, title='JD Rsik Login Confusion Matrix', threshold=0.5,
                              path=lstm_img_path)

    cmp_pr = []
    cmp_pr.extend([
        ('JD Rsik Login', y_test, y_pred_prob),
    ])

    # draw roc on xgboost
    pic_pr_cmp(title="JD Rsik Login PR Curve", file_path=lstm_img_path, y_data=cmp_pr)
    pic_pr_cmp_threshold(title="JD Rsik Login Pecision & Recall", file_path=lstm_img_path, y_data=cmp_pr)
    pic_roc_cmp(title="JD Rsik Login ROC", file_path=lstm_img_path, y_data=cmp_pr)
    ##f1 score
    best_threshold = pic_fbeta_cmp(title="JD Rsik Login Fbeta-Score", file_path=lstm_img_path, y_data=cmp_pr, beta=0.1)


if __name__ == '__main__':
    model_predict()
