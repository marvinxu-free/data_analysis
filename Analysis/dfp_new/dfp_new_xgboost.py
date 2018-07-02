# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com
#
"""
this file is used xgboost algorithm on shan yin dfp for ios system
"""
from __future__ import print_function, division
from Algorithm.dfp_xgboost import ios_dfp_xg, ios_dfp_p2, ios_dfp_p4
import warnings
from Utils.dfp_new.get_data import readData

warnings.filterwarnings("ignore")

# drop_cols = ['baseEntropy', 'device_browser_engine_entropy',
#              'device_model', 'jsid_entropy', 'resolution','ts_diff']
drop_cols = ['baseEntropy', 'device_model', 'jsid_entropy', 'resolution', 'ts_diff']


def dfp_main_part1(path, os='ios', part='part1'):
    """
    run xgboost for Part1,
    please see details in document
    :param file_name:
    :param os:
    :return:
    """
    df = readData(path=path, os_sys=os)

    print(df.columns[df.isnull().any()])
    need_cols = df.columns.difference(drop_cols)
    print("use {0} features".format(need_cols))

    df = df[need_cols]
    print("missing columns")
    df = df.fillna(-6.666)
    print(df.columns[df.isnull().any()])
    print()

    ios_dfp_xg(df=df, postfix=part)


def dfp_main_part2(file_name, test_file, os='ios', part='part2'):
    """
    run xgboost for Part2,
    please see details in document
    :param file_name:
    :param os:
    :return:
    """
    df = readData(file_=file_name, os_sys=os)
    df_test = readData(file_=test_file, os_sys=os)

    print(df.columns[df.isnull().any()])

    print("missing columns")
    df = df.fillna(-6.666)
    df_test = df_test.fillna(-6.666)
    print(df.columns[df.isnull().any()])
    print()

    need_cols = df.columns.difference(drop_cols)

    df = df[need_cols]
    df_test = df_test[need_cols]
    ios_dfp_p2(df=df, df_test=df_test, postfix=part)


def dfp_main_part4(file_name, test_file, os='ios', part='part4'):
    """
    run xgboost for Part4,
    please see details in document
    :param file_name:
    :param os:
    :return:
    """
    df = readData(file_=file_name, os_sys=os)
    df_test = readData(file_=test_file, os_sys=os)

    print(df.columns[df.isnull().any()])

    print("missing columns")
    df = df.fillna(-6.666)
    df_test = df_test.fillna(-6.666)
    print(df.columns[df.isnull().any()])
    print()

    need_cols = df.columns.difference(drop_cols)

    df = df[need_cols]
    df_test = df_test[need_cols]
    ios_dfp_p4(df=df, df_test=df_test, postfix=part)


if __name__ == "__main__":
    # file_path1 = "/Users/chaoxu/code/local-spark/Data/dfp_new/sample_2d"
    # dfp_main_part1(path=file_path1, os="ios", part="sample_2d")

    file_path2 = "/Users/chaoxu/code/local-spark/Data/dfp_new/sample_2d_p_r"
    dfp_main_part1(path=file_path2, os="ios", part="sample_2d_pr")

    file_path3 = "/Users/chaoxu/code/local-spark/Data/dfp_new/sample_2d_c"
    dfp_main_part1(path=file_path3, os="ios", part="sample_2d_cnr")

    file_path4 = "/Users/chaoxu/code/local-spark/Data/dfp_new/sample_2d_p"
    dfp_main_part1(path=file_path4, os="ios", part="sample_2d_pnr")
