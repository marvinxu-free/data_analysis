# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to analysis qiaoda data from telco use xgboost
"""
from __future__ import division, print_function, unicode_literals
import os
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from qiaoda_utils.format_check import format_check
from qiaoda_utils.qiaoda_telco_feature import *
from Algorithm.qiaoda_xgboost import qiaoda_xgb
from Algorithm.qiaoda_rf import qiaoda_rf
import logging
import warnings
from Params.path_params import Data_path

warnings.filterwarnings('ignore')
logging.getLogger("lda").setLevel(logging.WARNING)


def create_label(x):
    if x['fraud_type'] == 'qiancheng':
        return 1
    else:
        return 0


def get_data(fqiaoda, fmaxent, sfile):
    if os.path.exists(sfile):
        df_mt = pd.read_csv(sfile)
        return df_mt
    else:
        df_t = pd.read_excel(fqiaoda)
        df_t = df_t[df_t.columns[:11]]
        df_t = df_t.rename(columns={x: x.strip(" ") for x in df_t.columns}).rename(columns={u'原始IMEI': 'IMEI'})
        feature_obj_cols = df_t.select_dtypes(include=[np.object_]).columns.difference([u'IMEI'])
        print("feature columns is {0}".format(feature_obj_cols))
        df_t[feature_obj_cols] = df_t[feature_obj_cols].applymap(format_check)
        df_t[feature_obj_cols] = df_t[feature_obj_cols].astype(unicode)
        df_t[u'性别'] = df_t[u'基本特征'].apply(lambda x: get_sex(x))
        df_t[u'年龄下限'], df_t[u'年龄上限'] = zip(*df_t[u'基本特征'].apply(lambda x: get_ages(x)))
        df_t[u'学历'] = df_t[u'基本特征'].apply(lambda x: get_edu(x))
        df_t[u'经济水平'] = df_t[u'经济水平'].apply(lambda x: get_eco_level(x))
        df_t[u'消费特征'] = df_t[u'消费特征'].apply(lambda x: get_consume_feature(x))
        df_t[u'行为偏好'] = df_t[u'行为偏好'].apply(lambda x: get_act_feature(x))
        df_t[u'经常换卡'], df_t[u'使用小贷APP'] = zip(*df_t[u'个人信用'].apply(lambda x: get_credit(x)))
        df_t[u'年龄下限'] = df_t[u'年龄下限'].fillna(df_t[u'年龄下限'].median())
        df_t[u'年龄上限'] = df_t[u'年龄上限'].fillna(df_t[u'年龄上限'].median())

        df_m = pd.read_csv(fmaxent)
        df_m['label'] = df_m.apply(create_label, axis=1)
        df_m['IMEI'] = df_m['IMEI'].astype(str)
        df_t['IMEI'] = df_t['IMEI'].astype(str)
        df_mt = df_t.merge(df_m, how='left', on='IMEI')
        df_mt['label'] = df_mt['label'].fillna(0)
        df_mt = df_mt.drop([u'基本特征', u'个人信用', 'fraud_type', 'telco', u'在网状态'], axis=1)

        # feature_obj_cols = df_mt.select_dtypes(include=[np.object_]).columns.difference(['IMEI']).tolist()
        # feature_obj_cols += [u'消费特征', u'经常换卡', u'经济水平', u'行为偏好', u'使用小贷APP']
        use_cols = [u'使用小贷APP', u'在网时长', u'套餐品牌', u'是否实名', u'经常换卡', u'行为偏好']
        new_cols = []
        for col in use_cols:
            print(col)
            # number = LabelEncoder()
            # df_mt[col] = df_mt[col].astype(unicode)
            # df_mt[col] = number.fit_transform(df_mt[col])
            rename_dict = {x: "{0}_{1}".format(col, x) for x in df_mt[col].unique()}
            new_cols += rename_dict.values()
            print('rename dict is {0}'.format(rename_dict))
            df_col = pd.get_dummies(df_mt[col])
            df_mt = pd.concat([df_mt.drop(col, axis=1), df_col], axis=1)
            df_mt = df_mt.rename(columns=rename_dict)
        new_cols = new_cols + [u'label']
        print('really used cols is {0}'.format(new_cols))
        df_mt = df_mt[new_cols]
        df_mt.to_csv(sfile, encoding='utf-8', index=False)
        return df_mt


def dfp_main(post_fix='telco_rf_kf'):
    qiaoda_data_path = "{0}/qiaoda".format(Data_path)
    fqiaoda = "{0}/from_telco.xls".format(qiaoda_data_path)
    fmaxent = "{0}/qiaoda_imei_dec.csv".format(qiaoda_data_path)
    sfile = "{0}/ml_data.csv".format(qiaoda_data_path)
    df = get_data(fqiaoda, fmaxent, sfile)
    # qiaoda_xgb(df=df, postfix=post_fix)
    qiaoda_rf(df=df, postfix=post_fix)


if __name__ == '__main__':
    dfp_main()
