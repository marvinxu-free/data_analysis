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
from __future__ import print_function,division
from Algorithm.dfp_xgboost import ios_dfp_xg,ios_dfp_p2,ios_dfp_p4
import warnings
from Utils.shanyin.get_data import readData
from Params.path_params import Data_path
warnings.filterwarnings("ignore")

# drop_cols = ['baseEntropy', 'device_browser_engine_entropy',
#              'device_model', 'jsid_entropy', 'resolution','ts_diff']
drop_cols = ['baseEntropy',
             'device_model', 'jsid_entropy', 'resolution','ts_diff']

def dfp_main_part1(file_name, os='ios',part='part1'):
    """
    run xgboost for Part1,
    please see details in document
    :param file_name:
    :param os:
    :return:
    """
    df = readData(file_=file_name, os_sys=os)

    print(df.columns[df.isnull().any()])
    need_cols = df.columns.difference(drop_cols)

    df = df[need_cols]
    print("missing columns")
    df = df.fillna(-6.666)
    print(df.columns[df.isnull().any()])
    print()

    ios_dfp_xg(df=df,postfix=part)


def dfp_main_part2(file_name, test_file, os='ios',part='part2'):
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
    ios_dfp_p2(df=df,df_test=df_test,postfix=part)

def dfp_main_part4(file_name, test_file, os='ios',part='part4'):
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
    ios_dfp_p4(df=df,df_test=df_test,postfix=part)

if __name__ == "__main__":
    # file_p1 = "{0}/ios_sample.json.json".format(Data_path)

    # file_p1 = "{0}/ios_dfp_10/sample.json"
    # file_p2 = "{0}/ios_dfp_10/sample_test.json"
    # dfp_main_part1(file_name=file_p1, os='ios')
    # dfp_main_part2(file_name=file_p1, test_file=file_p2, os='ios')
    # dfp_main_part1(file_name=file_p2, os='ios',part='part3')
    # dfp_main_part4(file_name=file_p1, test_file=file_p2, os='ios')

    # file_p1 = "{0}/ios_dfp_10_C4000/sample.json"
    # file_p2 = "{0}/ios_dfp_10_C4000/sample_test.json"
    # dfp_main_part1(file_name=file_p1, os='ios',part="c4_part1")
    # dfp_main_part2(file_name=file_p1, test_file=file_p2, os='ios',part="c4_part2")
    # dfp_main_part1(file_name=file_p2, os='ios',part='c4_part3')
    # dfp_main_part4(file_name=file_p1, test_file=file_p2, os='ios',part="c4_part4")

    # file_p1 = "{0}/ios_dfp_10_4000/sample.json"
    # file_p2 = "{0}/ios_dfp_10_4000/sample_test.json"
    # dfp_main_part1(file_name=file_p1, os='ios',part="p4_part1")
    # dfp_main_part2(file_name=file_p1, test_file=file_p2, os='ios',part="p4_part2")
    # dfp_main_part1(file_name=file_p2, os='ios',part='p4_part3')
    # dfp_main_part4(file_name=file_p1, test_file=file_p2, os='ios',part="p4_part4")

    file_c1 = "{0}/shanyin_dfp/sample_c/data.json".format(Data_path)
    file_c2 = "{0}/shanyin_dfp/sample_c_test/data.json".format(Data_path)
    dfp_main_part1(file_name=file_c1, os='ios',part="c4_part1")
    # dfp_main_part2(file_name=file_c1, test_file=file_c2, os='ios',part="c4_part2")
    # dfp_main_part1(file_name=file_c2, os='ios',part='c4_part3')
    # dfp_main_part4(file_name=file_c1, test_file=file_c2, os='ios',part="c4_part4")
    #
    # file_p1 = "{0}/shanyin_dfp/sample_p.json"
    # file_p2 = "{0}/shanyin_dfp/sample_p_test.json"
    # dfp_main_part1(file_name=file_p1, os='ios',part="p4_part1")
    # dfp_main_part2(file_name=file_p1, test_file=file_p2, os='ios',part="p4_part2")
    # dfp_main_part1(file_name=file_p2, os='ios',part='p4_part3')
    # dfp_main_part4(file_name=file_p1, test_file=file_p2, os='ios',part="p4_part4")
