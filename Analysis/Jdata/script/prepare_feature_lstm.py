# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/5
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to prepare features for LSTE
"""

from __future__ import division, print_function
from feature_utils.lstm_feature import get_feature_lstm
from Params.jd_params import *
import os
import errno
from Pic.hist import pic_label
import re
import numpy as np
import json


def prepare_feature():
    df_train, df_test = get_feature_lstm(f1=train_login, f2=train_trade, time_unit='15Min', lstm_in=1, lstm_out=1)
    if not os.path.exists(lstm_img_path):
        try:
            os.makedirs(lstm_img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    pic_label(df=df_train, col='label', title=u"训练数据风险比例", file_path=lstm_img_path)
    pic_label(df=df_test, col='label', title=u"验证数据风险比例", file_path=lstm_img_path)
    df_train.to_csv(lstm_train_data)
    df_test.to_csv(lstm_test_data)

if __name__ == '__main__':
    prepare_feature()
