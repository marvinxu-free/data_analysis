# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com

"""
1.this file is used to get fraud maxent_id and save to file
2. original data has covert to device on spark
"""

from __future__ import print_function, division
import re

import numpy as np
import pandas as pd
from sklearn.externals import joblib
from Utils.qiancheng.get_data import read_dev_data
from Utils.common.MultiColumnLabelEncoder import MultiColumnLabelEncoder
import warnings
warnings.filterwarnings("ignore")


def df_main(df,os='ios'):
    df = df.loc[df.os == os]
    bool_cols = df.select_dtypes(include=[np.bool_]).columns.tolist()
    obj_cols = df.select_dtypes(include=[np.object_]).columns.tolist()
    encoder_cols = bool_cols + obj_cols
    if 'maxent_id' in encoder_cols:
        encoder_cols.remove('maxent_id')
    df = MultiColumnLabelEncoder(columns=encoder_cols).fit_transform(df)
    if os == 'ios':
        col_drop = ['os', 'aid_loan', 'imei_loan', 'mac_loan', 'imei_counts', 'mac_counts', 'aid_counts']
    else:
        col_drop = ['os','idfa_loan', 'idfa_counts', 'idfv_counts', 'imei_loan']
    df = df.drop(col_drop,axis=1)
    return df

def get_maxent_ids(df,os,path,num=None):
    print("get {0} maxent_id to {1}".format(os,path))
    csv_file = path + "/{0}_maxent_id.csv".format(os)
    label_csv_file = path + "/{0}_label_maxent_id.csv".format(os)
    qiancheng_maxent_ids_file = path + "/qiancheng_fraud_maxent_id.csv"
    df = df_main(df=df,os=os)
    df = df.reset_index(drop=True)
    maxent_id_all = df['maxent_id']
    print("{0} all maxent ids is {1}".format(os,maxent_id_all.shape))
    df = df.drop(['maxent_id'], axis=1)
    X_test = df.ix[:, df.columns != 'label']
    rf_model_path = '/Users/chaoxu/code/local-spark/Analysis/qiancheng/script/{0}_random_forest.pkl'\
        .format(os)
    clf_rf = joblib.load(rf_model_path)
    y_pred = clf_rf.predict(X_test)
    maxent_id_index = np.where(y_pred == 1)
    maxent_id = maxent_id_all.ix[maxent_id_index]
    qiancheng_maxent_ids = pd.read_csv(qiancheng_maxent_ids_file,names=['maxent_id'])
    if num is not None:
        maxent_id_sample = maxent_id.sample(n=num)
        fraud_qiancheng_index = qiancheng_maxent_ids['maxent_id'].isin(maxent_id_sample)
        fraud_qiancheng = qiancheng_maxent_ids[fraud_qiancheng_index]
        fraud_qiancheng.to_csv(path_or_buf=label_csv_file,index=False,header=False)
        maxent_id_sample.to_csv(path=csv_file,index=False,header=False)
    else:
        fraud_qiancheng_index = qiancheng_maxent_ids['maxent_id'].isin(maxent_id)
        fraud_qiancheng = qiancheng_maxent_ids[fraud_qiancheng_index]
        fraud_qiancheng.to_csv(path_or_buf=label_csv_file,index=False,header=False)
        maxent_id.to_csv(path=csv_file,index=False,header=False)

def main(file):
    df = read_dev_data(path=file)
    get_maxent_ids(df=df,os='android',\
                   path="/Users/chaoxu/code/local-spark/Data/qiancheng_data/check_data",\
                   num=10000)
    get_maxent_ids(df=df,os='ios', \
                   path="/Users/chaoxu/code/local-spark/Data/qiancheng_data/check_data",\
                   num=10000)

    # get_maxent_ids(df=df,os='android', \
    #                path="/Users/chaoxu/code/local-spark/Data/qiancheng_data/check_all_data", \
    #                num=None)
    # get_maxent_ids(df=df,os='ios', \
    #                path="/Users/chaoxu/code/local-spark/Data/qiancheng_data/check_all_data", \
    #                num=None)

if __name__ == "__main__":
    # file = "/Users/chaoxu/code/local-spark/Data/qiancheng_data/qiancheng_dev_merge/data.csv"
    file = "/Users/chaoxu/code/local-spark/Data/qiancheng_dev/qiancheng_dev_merge/data.csv"
    main(file=file)
