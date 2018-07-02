# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/3/12
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import print_function, division
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pandas as pd
import xlsxwriter
from Params.path_params import Document_path, Data_path
import os
import errno
import numpy as np
from collections import OrderedDict
from scipy.stats import beta
import networkx as nx


def binom_interval(success, total, confint=0.95):
    quantile = (1 - confint) / 2.
    lower = beta.ppf(quantile, success, total - success + 1)
    upper = beta.ppf(1 - quantile, success + 1, total - success)
    return (lower, upper)


credit_map = {
    0: u'逾期',
    1: u'正常'
}

pass_map = {
    0: u'拒绝',
    1: u'通过'
}


def read_data(xls_file):
    """
    this funtion used to read excel fro samoyed
    :param xls_file:
    :return:
    """
    df1 = pd.read_excel(io=xls_file, sheet_name=0)
    return df1


def get_pass_state(df, writer, os_name, thresholds):
    """
    this funtion used to state pass information
    :param df:
    :param xls_file:
    :param os_name:
    :param thresholds:
    :return:
    """
    state_dict = OrderedDict()
    for th in thresholds:
        df1 = df.loc[df[u'关联度'] >= th]
        state_dict.setdefault(u'阈值', []).append(th)
        state_dict.setdefault(u'设备1申请通过数量', []).append(df1.loc[df1[u'设备1申请状态'] == 1].shape[0])
        state_dict.setdefault(u'设备1申请拒绝数量', []).append(df1.loc[df1[u'设备1申请状态'] == 0].shape[0])
        state_dict.setdefault(u'设备1未申请数量', []).append(df1.loc[df1[u'设备1申请状态'].isnull()].shape[0])
        state_dict.setdefault(u'设备1申请通过率', []).append(
            df1.loc[df1[u'设备1申请状态'] == 1].shape[0] / df1.loc[df1[u'设备1申请状态'].notnull()].shape[0]
            if df1.loc[df1[u'设备1申请状态'].notnull()].shape[0] != 0 else None)
        state_dict.setdefault(u'设备2申请通过数量', []).append(df1.loc[df1[u'设备2申请状态'] == 1].shape[0])
        state_dict.setdefault(u'设备2申请拒绝数量', []).append(df1.loc[df1[u'设备2申请状态'] == 0].shape[0])
        state_dict.setdefault(u'设备2未申请数量', []).append(df1.loc[df1[u'设备2申请状态'].isnull()].shape[0])
        state_dict.setdefault(u'设备2申请通过率', []).append(
            df1.loc[df1[u'设备2申请状态'] == 1].shape[0] / df1.loc[df1[u'设备2申请状态'].notnull()].shape[0]
            if df1.loc[df1[u'设备2申请状态'].notnull()].shape[0] != 0 else None)
        state_dict.setdefault(u'设备匹配数量', []).append(df1.shape[0])
    pass_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    pass_df.to_excel(writer, u"{0}设备申请通过统计".format(os_name), index=False, engine=xlsxwriter)


def get_loan_state(df, writer, os_name, thresholds):
    """
    this funtion used to state loan information
    :param df:
    :param xls_file:
    :param os_name:
    :param thresholds:
    :return:
    """
    state_dict = OrderedDict()
    for th in thresholds:
        df1 = df.loc[df[u'关联度'] >= th]
        state_dict.setdefault(u'阈值', []).append(th)
        state_dict.setdefault(u'设备1还款正常数量', []).append(df1.loc[df1[u'设备1贷后表现'] == 1].shape[0])
        state_dict.setdefault(u'设备1还款逾期数量', []).append(df1.loc[df1[u'设备1贷后表现'] == 0].shape[0])
        state_dict.setdefault(u'设备1申请通过但未借款数量', []).append(
            df1.loc[(df1[u'设备1申请状态'] == 1) & (df1[u'设备1贷后表现'].isnull())].shape[0])
        state_dict.setdefault(u'设备1还款正常率', []).append(
            df1.loc[df1[u'设备1贷后表现'] == 1].shape[0] / df1.loc[df1[u'设备1贷后表现'].notnull()].shape[0]
            if df1.loc[df1[u'设备1贷后表现'].notnull()].shape[0] != 0 else None)
        state_dict.setdefault(u'设备2还款正常数量', []).append(df1.loc[df1[u'设备2贷后表现'] == 1].shape[0])
        state_dict.setdefault(u'设备2还款逾期数量', []).append(df1.loc[df1[u'设备2贷后表现'] == 0].shape[0])
        state_dict.setdefault(u'设备2申请通过但未借款数量', []).append(
            df1.loc[(df1[u'设备2申请状态'] == 1) & (df1[u'设备2贷后表现'].isnull())].shape[0])
        state_dict.setdefault(u'设备2还款正常率', []).append(
            df1.loc[df1[u'设备2贷后表现'] == 1].shape[0] / df1.loc[df1[u'设备2贷后表现'].notnull()].shape[0]
            if df1.loc[df1[u'设备2贷后表现'].notnull()].shape[0] != 0 else None)
        state_dict.setdefault(u'设备匹配数量', []).append(df1.shape[0])
    pass_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    pass_df.to_excel(writer, u"{0}设备逾期还款统计".format(os_name), index=False, engine=xlsxwriter)


def get_dev1_denied_dev2(df, os_name, thresholds):
    """
    设备1申请拒绝或者设备1逾期情况下，设备2的申请以及逾期统计
    :param df:
    :param writer:
    :param os_name:
    :param thresholds:
    :return:
    """
    state_dict = OrderedDict()
    df = df.loc[(df[u'设备1贷后表现'] == 0) | (df[u'设备1申请状态'] == 0)]
    for th in thresholds:
        df1 = df.loc[df[u'关联度'] >= th]
        state_dict.setdefault(u'系统', []).append(os_name)
        state_dict.setdefault(u'阈值', []).append(th)
        state_dict.setdefault(u'设备2申请通过数量', []).append(df1.loc[df1[u'设备2申请状态'] == 1].shape[0])
        state_dict.setdefault(u'设备2申请拒绝数量', []).append(df1.loc[df1[u'设备2申请状态'] == 0].shape[0])
        state_dict.setdefault(u'设备2未申请数量', []).append(df1.loc[df1[u'设备2申请状态'].isnull()].shape[0])
        state_dict.setdefault(u'设备2申请拒绝率', []).append(
            df1.loc[df1[u'设备2申请状态'] == 0].shape[0] / df1.loc[df1[u'设备2申请状态'].notnull()].shape[0]
            if df1.loc[df1[u'设备2申请状态'].notnull()].shape[0] != 0 else None)
        state_dict.setdefault(u'设备2还款正常数量', []).append(df1.loc[df1[u'设备2贷后表现'] == 1].shape[0])
        state_dict.setdefault(u'设备2还款逾期数量', []).append(df1.loc[df1[u'设备2贷后表现'] == 0].shape[0])
        state_dict.setdefault(u'设备2申请通过但未借款数量', []).append(
            df1.loc[(df1[u'设备2申请状态'] == 1) & (df1[u'设备2贷后表现'].isnull())].shape[0])
        state_dict.setdefault(u'设备2还款逾期率', []).append(
            df1.loc[df1[u'设备2贷后表现'] == 0].shape[0] / df1.loc[df1[u'设备2贷后表现'].notnull()].shape[0]
            if df1.loc[df1[u'设备2贷后表现'].notnull()].shape[0] != 0 else None)
        state_dict.setdefault(u'设备匹配数量', []).append(df1.shape[0])
    dev1_deny_dev2_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    return dev1_deny_dev2_df
    # dev1_deny_dev2_df.to_excel(writer, u"{0}设备1拒绝或逾期设备2统计情况".format(os_name), index=False, engine=xlsxwriter)


def dev1_concat_dev2(df, writer, os_name):
    """
    忽略相似度，将设备1与设备2联合起来考虑
    :param df:
    :param writer:
    :param os_name:
    :return:
    """
    state_dict = OrderedDict()
    df1 = pd.DataFrame(np.concatenate([df[[u'设备1 tick', u'设备1申请状态', u'设备1贷后表现']].values,
                                       df[[u'设备2 tick', u'设备2申请状态', u'设备2贷后表现']].values]),
                       columns=[u'设备tick', u'设备申请状态', u'设备贷后表现'])
    df1 = df1.drop_duplicates([u'设备tick', u'设备申请状态', u'设备贷后表现'])
    state_dict.setdefault(u'设备申请通过数量', []).append(df1.loc[df1[u'设备申请状态'] == 1].shape[0])
    state_dict.setdefault(u'设备申请拒绝数量', []).append(df1.loc[df1[u'设备申请状态'] == 0].shape[0])
    state_dict.setdefault(u'设备未申请数量', []).append(df1.loc[df1[u'设备申请状态'].isnull()].shape[0])
    state_dict.setdefault(u'设备申请通过率', []).append(
        df1.loc[df1[u'设备申请状态'] == 1].shape[0] / df1.loc[df1[u'设备申请状态'].notnull()].shape[0]
        if df1.loc[df1[u'设备申请状态'].notnull()].shape[0] != 0 else None)
    state_dict.setdefault(u'设备还款正常数量', []).append(df1.loc[df1[u'设备贷后表现'] == 1].shape[0])
    state_dict.setdefault(u'设备还款逾期数量', []).append(df1.loc[df1[u'设备贷后表现'] == 0].shape[0])
    state_dict.setdefault(u'设备申请通过但未借款数量', []).append(
        df1.loc[(df1[u'设备申请状态'] == 1) & (df1[u'设备贷后表现'].isnull())].shape[0])
    state_dict.setdefault(u'设备还款正常率', []).append(
        df1.loc[df1[u'设备贷后表现'] == 1].shape[0] / df1.loc[df1[u'设备贷后表现'].notnull()].shape[0]
        if df1.loc[df1[u'设备贷后表现'].notnull()].shape[0] != 0 else None)
    state_dict.setdefault(u'设备匹配数量', []).append(df1.shape[0])
    dev1_dev2_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    dev1_dev2_df.to_excel(writer, u"{0}设备整体统计情况".format(os_name), index=False, engine=xlsxwriter)


def dev1_concat_dev2_rej_overdude(df, os_name):
    """
    忽略相似度，将设备1与设备2联合起来考虑
    :param df:
    :param writer:
    :param os_name:
    :return:
    """
    state_dict = OrderedDict()
    df1 = pd.DataFrame(np.concatenate([df[[u'设备1 tick', u'设备1申请状态', u'设备1贷后表现']].values,
                                       df[[u'设备2 tick', u'设备2申请状态', u'设备2贷后表现']].values]),
                       columns=[u'设备tick', u'设备申请状态', u'设备贷后表现'])
    # df1 = df1.drop_duplicates([u'设备tick', u'设备申请状态', u'设备贷后表现'])
    df1 = df1.drop_duplicates([u'设备tick'])
    state_dict.setdefault(u'设备申请通过数量', []).append(df1.loc[df1[u'设备申请状态'] == 1].shape[0])
    state_dict.setdefault(u'设备申请拒绝数量', []).append(df1.loc[df1[u'设备申请状态'] == 0].shape[0])
    state_dict.setdefault(u'设备未申请数量', []).append(df1.loc[df1[u'设备申请状态'].isnull()].shape[0])

    dev_apply_rej_ratio = df1.loc[df1[u'设备申请状态'] == 0].shape[0] / df1.loc[df1[u'设备申请状态'].notnull()].shape[0] \
        if df1.loc[df1[u'设备申请状态'].notnull()].shape[0] != 0 else None

    sample_enough_high = dev_apply_rej_ratio + 2 * np.sqrt(dev_apply_rej_ratio * (1 - dev_apply_rej_ratio) /
                                                           df1.loc[df1[u'设备申请状态'] == 0].shape[0]) if \
        df1.loc[df1[u'设备申请状态'] == 0].shape[0] != 0 else None
    sample_enough_low = dev_apply_rej_ratio - 2 * np.sqrt(dev_apply_rej_ratio * (1 - dev_apply_rej_ratio) /
                                                          df1.loc[df1[u'设备申请状态'] == 0].shape[0]) if \
        df1.loc[df1[u'设备申请状态'] == 0].shape[0] != 0 else None
    if sample_enough_high < 1 and sample_enough_low > 0:
        print(u"样本数量足够")
    else:
        print(u'需要更多样本')

    dev_apply_rej_ratio_low = dev_apply_rej_ratio - 1.96 * np.sqrt(
        dev_apply_rej_ratio * (1 - dev_apply_rej_ratio) /
        df1.loc[df1[u'设备申请状态'] == 0].shape[0])
    dev_apply_rej_ratio_high = dev_apply_rej_ratio + 1.96 * np.sqrt(
        dev_apply_rej_ratio * (1 - dev_apply_rej_ratio) /
        df1.loc[df1[u'设备申请状态'] == 0].shape[0])
    state_dict.setdefault(u'设备申请拒绝率', []).append(dev_apply_rej_ratio)
    state_dict.setdefault(u'设备申请拒绝率下限', []).append(dev_apply_rej_ratio_low)
    state_dict.setdefault(u'设备申请拒绝率上限', []).append(dev_apply_rej_ratio_high)

    state_dict.setdefault(u'设备还款正常数量', []).append(df1.loc[df1[u'设备贷后表现'] == 1].shape[0])
    state_dict.setdefault(u'设备还款逾期数量', []).append(df1.loc[df1[u'设备贷后表现'] == 0].shape[0])
    state_dict.setdefault(u'设备申请通过但未借款数量', []).append(
        df1.loc[(df1[u'设备申请状态'] == 1) & (df1[u'设备贷后表现'].isnull())].shape[0])

    dev_overdude_ratio = df1.loc[df1[u'设备贷后表现'] == 0].shape[0] / df1.loc[df1[u'设备贷后表现'].notnull()].shape[0] \
        if df1.loc[df1[u'设备贷后表现'].notnull()].shape[0] != 0 else None
    dev_overdude_ratio_low, dev_overdude_ratio_high = binom_interval(df1.loc[df1[u'设备贷后表现'] == 0].shape[0],
                                                                     df1.loc[df1[u'设备贷后表现'].notnull()].shape[0])
    state_dict.setdefault(u'系统', []).append(os_name)
    state_dict.setdefault(u'设备还款逾期率', []).append(dev_overdude_ratio)
    state_dict.setdefault(u'设备还款逾期率下限', []).append(dev_overdude_ratio_low)
    state_dict.setdefault(u'设备还款逾期率上限', []).append(dev_overdude_ratio_high)

    state_dict.setdefault(u'设备匹配数量', []).append(df1.shape[0])
    dev1_dev2_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    return dev1_dev2_df
    # dev1_dev2_df.to_excel(writer, u"{0}设备整体拒绝和逾期统计情况".format(os_name), index=False, engine=xlsxwriter)


def xls_conficence_interval(df, thresholds, os_name):
    """
    本函数统计os_name对应的拒绝率，逾期率以及置信区间
    :param df:
    :param writer:
    :param os_name:
    :return:
    """
    state_dict = OrderedDict()
    for th in thresholds:
        df1 = df.loc[df[u'关联度'] >= th]
        state_dict.setdefault(u'系统', []).append(os_name)
        state_dict.setdefault(u'阈值', []).append(th)
        # 申请拒绝统计
        dev1_df = df1[[u'设备1 tick', u'设备1申请状态', u'设备1贷后表现']].drop_duplicates([u'设备1 tick'])
        dev2_df = df1[[u'设备2 tick', u'设备2申请状态', u'设备2贷后表现']].drop_duplicates([u'设备2 tick'])
        state_dict.setdefault(u'设备1申请通过数量', []).append(dev1_df.loc[dev1_df[u'设备1申请状态'] == 1].shape[0])
        state_dict.setdefault(u'设备1申请拒绝数量', []).append(dev1_df.loc[dev1_df[u'设备1申请状态'] == 0].shape[0])
        state_dict.setdefault(u'设备1未申请数量', []).append(dev1_df.loc[dev1_df[u'设备1申请状态'].isnull()].shape[0])
        dev1_apply_rej_ratio = dev1_df.loc[dev1_df[u'设备1申请状态'] == 0].shape[0] / \
                               dev1_df.loc[dev1_df[u'设备1申请状态'].notnull()].shape[0] \
            if dev1_df.loc[dev1_df[u'设备1申请状态'].notnull()].shape[0] != 0 else None
        state_dict.setdefault(u'设备1申请拒绝率', []).append(dev1_apply_rej_ratio)
        dev1_apply_rej_ratio_low = dev1_apply_rej_ratio - 1.96 * np.sqrt(
            dev1_apply_rej_ratio * (1 - dev1_apply_rej_ratio) /
            dev1_df.loc[dev1_df[u'设备1申请状态'] == 0].shape[0]) if dev1_df.loc[dev1_df[u'设备1申请状态'] == 0].shape[
                                                                   0] != 0 else None
        dev1_apply_rej_ratio_high = dev1_apply_rej_ratio + 1.96 * np.sqrt(
            dev1_apply_rej_ratio * (1 - dev1_apply_rej_ratio) /
            dev1_df.loc[dev1_df[u'设备1申请状态'] == 0].shape[0]) if dev1_df.loc[dev1_df[u'设备1申请状态'] == 0].shape[
                                                                   0] != 0 else None
        state_dict.setdefault(u'设备1申请拒绝率下限', []).append(dev1_apply_rej_ratio_low)
        state_dict.setdefault(u'设备1申请拒绝率上限', []).append(dev1_apply_rej_ratio_high)

        state_dict.setdefault(u'设备2申请通过数量', []).append(dev2_df.loc[dev2_df[u'设备2申请状态'] == 1].shape[0])
        state_dict.setdefault(u'设备2申请拒绝数量', []).append(dev2_df.loc[dev2_df[u'设备2申请状态'] == 0].shape[0])
        state_dict.setdefault(u'设备2未申请数量', []).append(dev2_df.loc[dev2_df[u'设备2申请状态'].isnull()].shape[0])
        dev2_apply_rej_ratio = dev2_df.loc[dev2_df[u'设备2申请状态'] == 0].shape[0] / \
                               dev2_df.loc[dev2_df[u'设备2申请状态'].notnull()].shape[0] \
            if dev2_df.loc[dev2_df[u'设备2申请状态'].notnull()].shape[0] != 0 else None
        state_dict.setdefault(u'设备2申请拒绝率', []).append(dev2_apply_rej_ratio)
        dev2_apply_rej_ratio_low = dev2_apply_rej_ratio - 1.96 * np.sqrt(
            dev2_apply_rej_ratio * (1 - dev2_apply_rej_ratio) /
            dev2_df.loc[dev2_df[u'设备2申请状态'] == 0].shape[0]) if dev2_df.loc[dev2_df[u'设备2申请状态'] == 0].shape[
                                                                   0] != 0 else None
        dev2_apply_rej_ratio_high = dev2_apply_rej_ratio + 1.96 * np.sqrt(
            dev2_apply_rej_ratio * (1 - dev2_apply_rej_ratio) /
            dev2_df.loc[dev2_df[u'设备2申请状态'] == 0].shape[0]) if dev2_df.loc[dev2_df[u'设备2申请状态'] == 0].shape[
                                                                   0] != 0 else None
        state_dict.setdefault(u'设备2申请拒绝率下限', []).append(dev2_apply_rej_ratio_low)
        state_dict.setdefault(u'设备2申请拒绝率上限', []).append(dev2_apply_rej_ratio_high)

        # 还款逾期统计
        state_dict.setdefault(u'设备1还款正常数量', []).append(dev1_df.loc[dev1_df[u'设备1贷后表现'] == 1].shape[0])
        state_dict.setdefault(u'设备1还款逾期数量', []).append(dev1_df.loc[dev1_df[u'设备1贷后表现'] == 0].shape[0])
        state_dict.setdefault(u'设备1申请通过但未借款数量', []).append(
            dev1_df.loc[(dev1_df[u'设备1申请状态'] == 1) & (dev1_df[u'设备1贷后表现'].isnull())].shape[0])

        dev1_overdude_ratio = dev1_df.loc[dev1_df[u'设备1贷后表现'] == 0].shape[0] / \
                              dev1_df.loc[dev1_df[u'设备1贷后表现'].notnull()].shape[0] \
            if dev1_df.loc[dev1_df[u'设备1贷后表现'].notnull()].shape[0] != 0 else None
        dev1_overdude_ratio_low, dev1_overdude_ratio_high = binom_interval(
            dev1_df.loc[dev1_df[u'设备1贷后表现'] == 0].shape[0],
            dev1_df.loc[dev1_df[u'设备1贷后表现'].notnull()].shape[0])
        state_dict.setdefault(u'设备1还款逾期率', []).append(dev1_overdude_ratio)
        state_dict.setdefault(u'设备1还款逾期率下限', []).append(dev1_overdude_ratio_low)
        state_dict.setdefault(u'设备1还款逾期率上限', []).append(dev1_overdude_ratio_high)

        state_dict.setdefault(u'设备2还款正常数量', []).append(dev2_df.loc[dev2_df[u'设备2贷后表现'] == 1].shape[0])
        state_dict.setdefault(u'设备2还款逾期数量', []).append(dev2_df.loc[dev2_df[u'设备2贷后表现'] == 0].shape[0])
        state_dict.setdefault(u'设备2申请通过但未借款数量', []).append(
            dev2_df.loc[(dev2_df[u'设备2申请状态'] == 1) & (dev2_df[u'设备2贷后表现'].isnull())].shape[0])

        dev2_overdude_ratio = dev2_df.loc[dev2_df[u'设备2贷后表现'] == 0].shape[0] / \
                              dev2_df.loc[dev2_df[u'设备2贷后表现'].notnull()].shape[0] \
            if dev2_df.loc[dev2_df[u'设备2贷后表现'].notnull()].shape[0] != 0 else None
        dev2_overdude_ratio_low, dev2_overdude_ratio_high = binom_interval(
            dev2_df.loc[dev2_df[u'设备2贷后表现'] == 0].shape[0],
            dev2_df.loc[dev2_df[u'设备2贷后表现'].notnull()].shape[0])
        state_dict.setdefault(u'设备2还款逾期率', []).append(dev2_overdude_ratio)
        state_dict.setdefault(u'设备2还款逾期率下限', []).append(dev2_overdude_ratio_low)
        state_dict.setdefault(u'设备2还款逾期率上限', []).append(dev2_overdude_ratio_high)

        # state_dict.setdefault(u'设备匹配数量', []).append(df1.shape[0])
    pass_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    return pass_df
    # pass_df.to_excel(writer, u"{0}设备拒绝和逾期统计分析".format(os_name), index=False, engine=xlsxwriter)


def report_graph(df, os_name, thresholds):
    """

    :param df:
    :param os_name:
    :param thresholds:
    :param xls_str:
    :return:
    """
    g = nx.Graph()
    for index, row in df.iterrows():
        g.add_edge(row[u'设备1 tick'], row[u'设备2 tick'], similar=row[u'关联度'])
    nodes = nx.connected_components(g)
    nodes_connect_fraud = filter(lambda x: len(x) >= 3, nodes)
    nodes_fraud = set([])
    for x in nodes_connect_fraud:
        if x is not None:
            nodes_fraud.update(x)
    df1 = df.loc[(df[u'设备1 tick'].isin(nodes_fraud)) | (df[u'设备2 tick'].isin(nodes_fraud))]
    dev1_concat_dev2_df = dev1_concat_dev2_rej_overdude(df=df1, os_name=os_name)
    pass_df = xls_conficence_interval(df=df1, os_name=os_name, thresholds=thresholds)
    dev1_denied_dev2 = get_dev1_denied_dev2(df=df1, os_name=os_name, thresholds=thresholds)
    return dev1_concat_dev2_df, pass_df, dev1_denied_dev2


def xls_sheets(os_name='ios'):
    if os_name == 'ios':
        data_file = u"{0}/smy_dev_loan_corr/ios_data_smy.xlsx".format(Document_path)
        df = read_data(data_file)
    else:
        data_file = u"{0}/smy_dev_loan_corr/android_data_v2_smy.xlsx".format(Document_path)
        df = read_data(data_file)
    print(data_file)
    thresholds = [x / 100.0 for x in range(90, 100)]
    # thresholds = [0.9]
    dev1_concat_dev2_df = dev1_concat_dev2_rej_overdude(df=df, os_name=os_name)
    pass_df = xls_conficence_interval(df=df, os_name=os_name, thresholds=thresholds)
    dev1_denied_dev2 = get_dev1_denied_dev2(df=df, os_name=os_name, thresholds=thresholds)
    g_dev1_concat_dev2_df, g_pass_df, g_dev1_denied_dev2 = report_graph(df=df, os_name=os_name, thresholds=thresholds)
    return dev1_concat_dev2_df, pass_df, dev1_denied_dev2, g_dev1_concat_dev2_df, g_pass_df, g_dev1_denied_dev2


def xls_main(writer):
    ios_dfs = xls_sheets('ios')
    android_dfs = xls_sheets('android')
    all_dfs = []
    sheet_names = [
        u'设备整体拒绝和逾期统计情况',
        u'设备拒绝和逾期统计分析',
        u'设备1拒绝或逾期设备2统计情况',
        u'聚集度>=3,设备整体拒绝和逾期统计情况',
        u'聚集度>=3,设备拒绝和逾期统计分析',
        u'聚集度>=3,设备1拒绝或逾期设备2统计情况'
    ]
    for x, y in zip(ios_dfs, android_dfs):
        df = pd.concat([x, y], axis=0)
        all_dfs.append(df)

    for s, d in zip(sheet_names, all_dfs):
        if u'阈值' in d.columns.tolist():
            d = d.set_index([u'系统', u'阈值'])
        else:
            d = d.set_index([u'系统'])
        d.to_excel(writer, s, engine=xlsxwriter)


if __name__ == '__main__':
    # excel_file = u"{0}/smy_dev_loan_corr/萨摩耶设备关联授信分析.xlsx".format(Document_path)
    # excel_file = u"{0}/smy_dev_loan_corr/萨摩耶设备关联统计分析.xlsx".format(Document_path)
    excel_file = u"{0}/smy_dev_loan_corr/萨摩耶设备关联统计分析v4.xlsx".format(Document_path)
    writer = pd.ExcelWriter(excel_file)
    xls_main(writer=writer)
    writer.save()
