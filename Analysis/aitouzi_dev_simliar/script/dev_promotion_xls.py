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

promotion_map = {
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


def concat_unique_promotion(df_in, thresholds, os_name, unique_col='user_id'):
    """
    忽略相似度，将设备1与设备2联合起来考虑
    :param df:
    :param thresholds:
    :param os_name:
    :param unique_col:
    :return:
    """
    state_dict = OrderedDict()
    for th in thresholds:
        state_dict.setdefault(u'系统', []).append(os_name)
        df = df_in.loc[(df_in[u'关联度'] >= th[0]) & (df_in[u'关联度'] < th[1])]
        state_dict.setdefault(u'阈值', []).append("[{0},{1})".format(th[0], th[1]))
        df1 = pd.DataFrame(np.concatenate([df[[u'设备1tick', u'设备1是否疑似推广欺诈', u'设备1user_id']].values,
                                           df[[u'设备2tick', u'设备2是否疑似推广欺诈', u'设备2user_id']].values]),
                           columns=[u'设备tick', u'设备是否疑似推广欺诈', 'user_id'])
        if unique_col == 'user_id':
            xls_str = u'用户'
            df1 = df1.drop_duplicates([u'user_id'])
        else:
            xls_str = u'设备'
            df1 = df1.drop_duplicates([u'设备tick'])

        state_dict.setdefault(u'欺诈数量'.format(xls_str), []).append(df1.loc[df1[u'设备是否疑似推广欺诈'] == 1].shape[0])
        state_dict.setdefault(u'非欺诈数量'.format(xls_str), []).append(df1.loc[df1[u'设备是否疑似推广欺诈'] == 0].shape[0])

        promotion_ratio = df1.loc[df1[u'设备是否疑似推广欺诈'] == 1].shape[0] / df1.loc[df1[u'设备是否疑似推广欺诈'].notnull()].shape[0] \
            if df1.loc[df1[u'设备是否疑似推广欺诈'].notnull()].shape[0] != 0 else None

        sample_enough_high = df1[u'设备是否疑似推广欺诈'].notnull().shape[0] * promotion_ratio
        sample_enough_low = df1[u'设备是否疑似推广欺诈'].notnull().shape[0] * (1 - promotion_ratio)
        use_z = True
        if sample_enough_high >= 5 and sample_enough_low >= 5:
            print(u"{0}{1}整体样本数量足够".format(os_name, xls_str))
        else:
            print(u'{0}{1}整体需要更多样本'.format(os_name, xls_str))
            use_z = False

        if use_z:
            promotion_ratio_low = promotion_ratio - 1.96 * np.sqrt(
                promotion_ratio * (1 - promotion_ratio) /
                df1.loc[df1[u'设备是否疑似推广欺诈'].notnull()].shape[0])
            promotion_ratio_high = promotion_ratio + 1.96 * np.sqrt(
                promotion_ratio * (1 - promotion_ratio) /
                df1.loc[df1[u'设备是否疑似推广欺诈'].notnull()].shape[0])
        else:
            promotion_ratio_low, promotion_ratio_high = binom_interval(df1.loc[df1[u'设备是否疑似推广欺诈'] == 1].shape[0],
                                                                       df1.loc[df1[u'设备是否疑似推广欺诈'].notnull()].shape[0])
        state_dict.setdefault(u'欺诈比例', []).append(promotion_ratio)
        state_dict.setdefault(u'欺诈比例下限', []).append(promotion_ratio_low)
        state_dict.setdefault(u'欺诈比例上限', []).append(promotion_ratio_high)
        state_dict.setdefault(u'样本数量', []).append(df1.shape[0])

    dev1_dev2_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    return dev1_dev2_df


def xls_conficence_interval(df, thresholds, os_name, unique_col='user_id'):
    """
    本函数统计os_name对应的拒绝率，逾期率以及置信区间
    :param df:
    :param os_name:
    :return:
    """
    state_dict = OrderedDict()
    chk_dict = {
        'user_id': {
            'dev1': [u'设备1user_id', u'设备1是否疑似推广欺诈', ],
            'dev2': [u'设备2user_id', u'设备2是否疑似推广欺诈', ]
        },
        'dev': {
            'dev1': [u'设备1tick', u'设备1是否疑似推广欺诈', ],
            'dev2': [u'设备2tick', u'设备2是否疑似推广欺诈', ]
        }
    }

    for th in thresholds:
        df1 = df.loc[(df[u'关联度'] >= th[0]) & (df[u'关联度'] < th[1])]
        state_dict.setdefault(u'阈值', []).append("[{0},{1})".format(th[0], th[1]))
        state_dict.setdefault(u'系统', []).append(os_name)
        if unique_col == 'user_id':
            xls_str = u'用户'
            dev1_df = df1[chk_dict['user_id']['dev1']].drop_duplicates([chk_dict['user_id']['dev1'][0]])
            dev2_df = df1[chk_dict['user_id']['dev2']].drop_duplicates(chk_dict['user_id']['dev2'][0])
        elif unique_col == 'dev':
            xls_str = u'设备'
            dev1_df = df1[chk_dict['dev']['dev1']].drop_duplicates([chk_dict['dev']['dev1'][0]])
            dev2_df = df1[chk_dict['dev']['dev2']].drop_duplicates(chk_dict['dev']['dev2'][0])
        else:
            print("error base col")
            return None

        state_dict.setdefault(u'{0}1欺诈数量'.format(xls_str), []).append(
            dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]] == 1].shape[0])
        state_dict.setdefault(u'{0}1非欺诈数量'.format(xls_str), []).append(
            dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]] == 0].shape[0])
        dev1_promotion_fraud_ratio = dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]] == 1].shape[0] / \
                                     dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[0] \
            if dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[0] != 0 else None
        state_dict.setdefault(u'{0}1推广欺诈率'.format(xls_str), []).append(dev1_promotion_fraud_ratio)

        dev1_sample_enough_high = dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[
                                      0] * dev1_promotion_fraud_ratio
        dev1_sample_enough_low = dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[0] * (
            1 - dev1_promotion_fraud_ratio)

        use_z = True
        if dev1_sample_enough_high >= 5 and dev1_sample_enough_low >= 5:
            print(u"{0}{1}设备1样本数量足够".format(os_name, xls_str))
        else:
            print(u'{0}{1}设备1需要更多样本'.format(os_name, xls_str))
            use_z = False

        if use_z:
            dev1_promotion_fraud_ratio_low = dev1_promotion_fraud_ratio - 1.96 * np.sqrt(
                dev1_promotion_fraud_ratio * (1 - dev1_promotion_fraud_ratio) /
                dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[0]) if \
                dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[
                    0] != 0 else None
            dev1_promotion_fraud_ratio_high = dev1_promotion_fraud_ratio + 1.96 * np.sqrt(
                dev1_promotion_fraud_ratio * (1 - dev1_promotion_fraud_ratio) /
                dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[0]) if \
                dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[
                    0] != 0 else None
        else:
            dev1_promotion_fraud_ratio_low, dev1_promotion_fraud_ratio_high = binom_interval(
                dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]] == 1].shape[0],
                dev1_df.loc[dev1_df[chk_dict[unique_col]['dev1'][1]].notnull()].shape[0])

        state_dict.setdefault(u'{0}1欺诈率下限'.format(xls_str), []).append(
            dev1_promotion_fraud_ratio_low)
        state_dict.setdefault(u'{0}1欺诈率上限'.format(xls_str), []).append(
            dev1_promotion_fraud_ratio_high)
        state_dict.setdefault(u'{0}1匹配数量'.format(xls_str), []).append(dev1_df.shape[0])

        # -------------------------------------设备2或者用户2---------------------------------------------------

        state_dict.setdefault(u'{0}2欺诈数量'.format(xls_str), []).append(
            dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]] == 1].shape[0])
        state_dict.setdefault(u'{0}2非欺诈数量'.format(xls_str), []).append(
            dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]] == 0].shape[0])
        dev2_promotion_fraud_ratio = dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]] == 1].shape[0] / \
                                     dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[0] \
            if dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[0] != 0 else None
        state_dict.setdefault(u'{0}2欺诈率'.format(xls_str), []).append(dev2_promotion_fraud_ratio)

        dev2_sample_enough_high = dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[
                                      0] * dev2_promotion_fraud_ratio
        dev2_sample_enough_low = dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[0] * (
            1 - dev2_promotion_fraud_ratio)
        use_z = True
        if dev2_sample_enough_high >= 5 and dev2_sample_enough_low >= 5:
            print(u"{0}{1}设备2样本数量足够".format(os_name, xls_str))
        else:
            print(u'{0}{1}设备2需要更多样本'.format(os_name, xls_str))
            use_z = False

        if use_z:
            dev2_promotion_fraud_ratio_low = dev2_promotion_fraud_ratio - 1.96 * np.sqrt(
                dev2_promotion_fraud_ratio * (1 - dev2_promotion_fraud_ratio) /
                dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[0]) if \
                dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[
                    0] != 0 else None
            dev2_promotion_fraud_ratio_high = dev2_promotion_fraud_ratio + 1.96 * np.sqrt(
                dev2_promotion_fraud_ratio * (1 - dev2_promotion_fraud_ratio) /
                dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[0]) if \
                dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[
                    0] != 0 else None
        else:
            dev2_promotion_fraud_ratio_low, dev2_promotion_fraud_ratio_high = binom_interval(
                dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]] == 1].shape[0],
                dev2_df.loc[dev2_df[chk_dict[unique_col]['dev2'][1]].notnull()].shape[0])

        state_dict.setdefault(u'{0}欺诈率下限'.format(xls_str), []).append(
            dev2_promotion_fraud_ratio_low)
        state_dict.setdefault(u'{0}欺诈率上限'.format(xls_str), []).append(
            dev2_promotion_fraud_ratio_high)
        state_dict.setdefault(u'{0}匹配数量'.format(xls_str), []).append(dev2_df.shape[0])

    pass_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    return pass_df
    # pass_df.to_excel(writer, u"{0}推广欺诈{1}比率单独分析".format(os_name, xls_str), index=False, engine=xlsxwriter)


def get_dev1_fraud_dev2(df, os_name, thresholds, xls_str=u'设备1欺诈设备2统计'):
    """
    设备1推广欺诈的情况下，设备2的营销欺诈的比例
    :param df:
    :param writer:
    :param os_name:
    :param thresholds:
    :return:
    """
    state_dict = OrderedDict()
    df = df.loc[df[u'设备1是否疑似推广欺诈'] == 1]
    df = df.drop_duplicates([u'设备1tick', u'设备2tick', u'设备1是否疑似推广欺诈'])
    for th in thresholds:
        df1 = df.loc[(df[u'关联度'] >= th[0]) & (df[u'关联度'] < th[1])]
        state_dict.setdefault(u'阈值', []).append("[{0},{1})".format(th[0], th[1]))
        state_dict.setdefault(u'系统', []).append(os_name)
        state_dict.setdefault(u'设备2非欺诈数量', []).append(df1.loc[df1[u'设备2是否疑似推广欺诈'] == 0].shape[0])
        state_dict.setdefault(u'设备2欺诈数量', []).append(df1.loc[df1[u'设备2是否疑似推广欺诈'] == 1].shape[0])
        promotion_ratio = df1.loc[df1[u'设备2是否疑似推广欺诈'] == 1].shape[0] / df1.loc[df1[u'设备2是否疑似推广欺诈'].notnull()].shape[0] \
            if df1.loc[df1[u'设备2是否疑似推广欺诈'].notnull()].shape[0] != 0 else None

        sample_enough_high = df1[u'设备2是否疑似推广欺诈'].notnull().shape[0] * promotion_ratio
        sample_enough_low = df1[u'设备2是否疑似推广欺诈'].notnull().shape[0] * (1 - promotion_ratio)
        use_z = True
        if sample_enough_high >= 5 and sample_enough_low >= 5:
            print(u"{0}{1}整体样本数量足够".format(os_name, xls_str))
        else:
            print(u'{0}{1}整体需要更多样本'.format(os_name, xls_str))
            use_z = False

        if use_z:
            promotion_ratio_low = promotion_ratio - 1.96 * np.sqrt(
                promotion_ratio * (1 - promotion_ratio) /
                df1.loc[df1[u'设备2是否疑似推广欺诈'].notnull()].shape[0])
            promotion_ratio_high = promotion_ratio + 1.96 * np.sqrt(
                promotion_ratio * (1 - promotion_ratio) /
                df1.loc[df1[u'设备2是否疑似推广欺诈'].notnull()].shape[0])
        else:
            promotion_ratio_low, promotion_ratio_high = binom_interval(df1.loc[df1[u'设备2是否疑似推广欺诈'] == 1].shape[0],
                                                                       df1.loc[df1[u'设备2是否疑似推广欺诈'].notnull()].shape[0])
        state_dict.setdefault(u'设备2欺诈比例', []).append(promotion_ratio)
        state_dict.setdefault(u'设备2欺诈比例下限', []).append(promotion_ratio_low)
        state_dict.setdefault(u'设备2欺诈比例上限', []).append(promotion_ratio_high)
        state_dict.setdefault(u'样本数量', []).append(df1.shape[0])

    dev1_deny_dev2_df = pd.DataFrame.from_dict(state_dict, orient="columns")
    return dev1_deny_dev2_df


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
        g.add_edge(row[u'设备1tick'], row[u'设备2tick'], similar=row[u'关联度'])
    nodes = nx.connected_components(g)
    nodes_connect_fraud = filter(lambda x: len(x) >= 3, nodes)
    nodes_fraud = set([])
    for x in nodes_connect_fraud:
        if x is not None:
            nodes_fraud.update(x)
    # nodes_fraud = set(node_list)
    df1 = df.loc[(df[u'设备1tick'].isin(nodes_fraud)) | (df[u'设备2tick'].isin(nodes_fraud))]
    g_split_df = xls_conficence_interval(df=df1, thresholds=thresholds, os_name=os_name, unique_col='dev')
    g_total_df = concat_unique_promotion(df_in=df1, os_name=os_name, thresholds=thresholds, unique_col='dev')
    g_dev1_deny_df2 = get_dev1_fraud_dev2(df=df1, os_name=os_name, thresholds=thresholds, xls_str=u'打团数据')
    return g_total_df, g_split_df, g_dev1_deny_df2


def xls_sheets(writer, os_name='ios'):
    if os_name == 'ios':
        data_file = u"{0}/aitouzi_dev_simliar/爱投资_ios_推广评估_tag_20180327.xlsx".format(Document_path)
        df = read_data(data_file)
    else:
        data_file = u"{0}/aitouzi_dev_simliar/爱投资_android_推广评估_tag_20180327.xlsx".format(Document_path)
        df = read_data(data_file)
    print(data_file)
    # thresholds = [x / 100.0 for x in range(90, 100)]
    # thresholds.remove(0.95)
    thresholds = [(0, 0.95), (0.95, 1.1), (0.96, 1.1), (0.97, 1.1), (0.98, 1.1), (0.99, 1.1)]
    # all_user_df = concat_unique_promotion(df_in=df, thresholds=thresholds, os_name=os_name)
    all_dev_df = concat_unique_promotion(df_in=df, thresholds=thresholds, os_name=os_name, unique_col=u'dev')
    # user_df = xls_conficence_interval(df=df, os_name=os_name, thresholds=thresholds)
    dev_df = xls_conficence_interval(df=df, os_name=os_name, thresholds=thresholds, unique_col='dev')
    dev1_deny_dev2_df = get_dev1_fraud_dev2(df=df, os_name=os_name, thresholds=thresholds)
    g_total_df, g_split_df, g_dev1_deny_df2 = report_graph(df=df, os_name=os_name, thresholds=thresholds)
    return all_dev_df, dev_df, dev1_deny_dev2_df, g_total_df, g_split_df, g_dev1_deny_df2


def xls_main(writer):
    ios_dfs = xls_sheets(writer, 'ios')
    android_dfs = xls_sheets(writer, 'android')
    all_dfs = []
    sheet_names = [
        u'整体设备维度欺诈比例',
        u'设备维度欺诈比例',
        u'设备1欺诈情况下设备2欺诈比例',
        u'聚集度>=3,设备整体欺诈比例',
        u'聚集度>=3,设备欺诈比例',
        u'聚集度>=3,设备1欺诈情况下设备2欺诈比例'
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
    excel_file = u"{0}/aitouzi_dev_simliar/爱投资相似设备推广规则分析v9.xlsx".format(Document_path)
    writer = pd.ExcelWriter(excel_file)
    xls_main(writer)
    writer.save()
