# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to prepare feature from JD data
特征说明见： 建模思路.md
"""
from __future__ import print_function, division
from Params.jd_params import *
import argparse
from feature_utils.prepare_features_trade import prepare_features


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--prepare_mode', action='store', type=str, default='train', dest='prepare_mode',
                        help="prepare feature for train/test")
    FLAGS, _ = parser.parse_known_args()
    if FLAGS.prepare_mode == 'train':
        prepare_features(train_login, train_trade)
    else:
        prepare_features(train_login_test, train_trade_test)
