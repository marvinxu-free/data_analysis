# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/10
# Company : Maxent
# Email: chao.xu@maxent-inc.com

import pandas as pd
import numpy as np

"""
this file is used to check data sets to aviod data misMatch for ml model
some condition must meet:
1. label =1 ,shape is same
2. when os, label=1, all feature must be same from file1 and file2

some data sets such as have different label 1 is not suitable for this function
"""

def check_files(file1,file2, os=['ios', 'android'], target='label'):
    """
    check feature from two different files
    :param file1:
    :param file2:
    :param os:
    :param target:
    :return:
    """
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    for os_sys in os:
        print("check {0}".format(os_sys))
        os_mis_cols = check_features(df1=df1,df2=df2,os=os_sys,target=target)
        if len(os_mis_cols) >= 1:
            print("!!!{0} not same features:\n{1}\n".format(os_sys,os_mis_cols))
        else:
            print("^_^ {0}".format(os_sys))

def check_features(df1,df2,os,target='label'):
    """
    this function used to check feature per os or label
    :param df1:
    :param df2:
    :param os:
    :param target:
    :return: mis_cols
    """
    mis_cols = []
    #get df you want to check
    df1_check = df1.loc[(df1[target] == 1) & (df1.os == os)]
    df2_check = df2.loc[(df2[target] == 1) & (df2.os == os)]
    assert df1_check.shape == df2_check.shape,\
        "dataframe shape is not same of df1:{0} df2:{1}".format(df1_check.shape, df2_check.shape)

    #get object columns and bool
    bool_cols = df1_check.select_dtypes(include=[np.bool_]).columns.tolist()
    obj_cols = df1_check.select_dtypes(include=[np.object_]).columns.tolist()
    obj_chk_cols = bool_cols + obj_cols
    for col in obj_chk_cols:
        col1_chk = df1_check[col].value_counts()
        col2_chk = df2_check[col].value_counts()
        if np.allclose(col1_chk.values,col2_chk.values,rtol=1e-6):
            pass
        else:
            print("col {0} dtye={1} failed:\n{2}\n{3}".format(col, df1_check[col].dtype, col1_chk, col2_chk))
            mis_cols.append(col)

    # get numerical features
    num_chk_cols = df1_check.columns.difference(obj_chk_cols)
    for col in num_chk_cols:
        col1_chk = df1_check[col].describe()
        col2_chk = df2_check[col].describe()
        if np.allclose(col1_chk.values,col2_chk.values,rtol=1e-6):
            pass
        else:
            print("col {0} dtye={1} failed:\n{2}\n{3}".format(col, df1_check[col].dtype, col1_chk, col2_chk))
            mis_cols.append(col)
    return mis_cols

