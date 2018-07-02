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
from Utils.common.custerReadFile import read_multi_csv
import numpy as np
import re
from numpy import logical_or, logical_and
import pandas as pd
import xlsxwriter
from Params.path_params import Document_path, Data_path
from Pic.maxent_style import maxent_style
from Pic.pic_pie import pic_pie
import os
import errno
import networkx as nx
from matplotlib import pyplot as plt


def read_data(xls_file):
    """
    this funtion used to read excel fro samoyed
    :param xls_file:
    :return:
    """
    df1 = pd.read_excel(io=xls_file, sheet_name=0)
    return df1


def get_loan_rule_analysis(df1, img_path, os_name='ios'):
    """
    loan rule check
    :param df1:
    :return:
    """
    credit_map = {
        0: u'逾期',
        1: u'正常'
    }
    pass_map = {
        0: u'拒绝',
        1: u'通过'
    }
    ### 前面通过后面通过的比例
    value_counts_0 = df1.loc[df1[u'设备1申请状态'] == 1][u'设备2申请状态'].value_counts()
    values_0 = value_counts_0.values
    cats_0 = map(lambda x: pass_map[x], value_counts_0.index.tolist())
    pic_pie(values=values_0, cats=cats_0, title=u'设备1申请通过时设备2申请状态比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1申请通过时设备2申请状态比例'))

    value_counts_1 = df1.loc[df1[u'设备1申请状态'] == 0][u'设备2申请状态'].value_counts()
    values_1 = value_counts_1.values
    cats_1 = map(lambda x: pass_map[x], value_counts_1.index.tolist())
    pic_pie(values=values_1, cats=cats_1, title=u'设备1申请不通过时设备2申请状态比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1申请不通过时设备2申请状态比例'))

    value_counts_2 = df1.loc[df1[u'设备1申请状态'] == 1][u'设备2贷后表现'].value_counts()
    values_2 = value_counts_2.values
    cats_2 = map(lambda x: credit_map[x], value_counts_2.index.tolist())
    pic_pie(values=values_2, cats=cats_2, title=u'设备1申请通过时设备2贷后表现比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1申请通过时设备2贷后表现比例'))

    value_counts_3 = df1.loc[df1[u'设备1申请状态'] == 0][u'设备2贷后表现'].value_counts()
    values_3 = value_counts_3.values
    cats_3 = map(lambda x: credit_map[x], value_counts_3.index.tolist())
    pic_pie(values=values_3, cats=cats_3, title=u'设备1申请不通过时设备2贷后表现比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1申请不通过时设备2贷后表现比例'))

    value_counts_4 = df1.loc[df1[u'设备1贷后表现'] == 0][u'设备2贷后表现'].value_counts()
    values_4 = value_counts_4.values
    cats_4 = map(lambda x: credit_map[x], value_counts_4.index.tolist())
    pic_pie(values=values_4, cats=cats_4, title=u'设备1贷后逾期时设备2逾期比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1贷后逾期时设备2逾期比例'))

    value_counts_5 = df1.loc[df1[u'设备1贷后表现'] == 0][u'设备2申请状态'].value_counts()
    values_5 = value_counts_5.values
    cats_5 = map(lambda x: pass_map[x], value_counts_5.index.tolist())
    pic_pie(values=values_5, cats=cats_5, title=u'设备1贷后逾期时设备2过比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1贷后逾期时设备2通过比例'))

    value_counts_6 = df1.loc[df1[u'设备1贷后表现'] == 1][u'设备2贷后表现'].value_counts()
    values_6 = value_counts_6.values
    cats_6 = map(lambda x: credit_map[x], value_counts_6.index.tolist())
    pic_pie(values=values_6, cats=cats_6, title=u'设备1贷后不逾期时设备2逾期比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1贷后不逾期时设备2逾期比例'))

    value_counts_7 = df1.loc[df1[u'设备1贷后表现'] == 1][u'设备2申请状态'].value_counts()
    values_7 = value_counts_7.values
    cats_7 = map(lambda x: pass_map[x], value_counts_7.index.tolist())
    pic_pie(values=values_7, cats=cats_7, title=u'设备1贷后不逾期时设备2过比例',
            fname="{0}/{1}{2}.png".format(img_path, os_name, u'设备1贷后不逾期时设备2通过比例'))


def loan_graph(df):
    """

    :param df:
    :param img_path:
    :return:
    """
    df_new = df.loc[(df[u'设备1贷后表现'] == 0) | (df[u'设备2贷后表现'] == 0)]
    g = nx.Graph()
    for index, row in df_new.iterrows():
        g.add_edge(row[u'设备1 tick'], row[u'设备2 tick'], similar=row[u'关联度'])
    return g


@maxent_style
def loan_report(g, os_name, img_path=None, palette=None, dpi=600):
    """

    :param g:
    :param img_path:
    :return:
    """
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    pos = nx.spring_layout(g)
    node_colors = 'b'
    edge_colors = 'r'
    nx.draw(g, pos, ax=ax, node_color=node_colors, edge_color=edge_colors,
            width=2, edge_cmap=palette, with_labels=False, node_size=5)
    ax.set_title(u'逾期设备图形数据结构')
    fig.savefig(filename=u"{0}/{1}逾期设备图形数据结构.png".format(img_path, os_name), dpi=dpi, format='png')
    plt.show(block=False)


def pic_main(os_name='ios'):
    if os_name == 'ios':
        data_file = u"{0}/smy_dev_loan_corr/ios_data_smy.xlsx".format(Document_path)
        df = read_data(data_file)
    else:
        data_file = u"{0}/smy_dev_loan_corr/android_data_v2_smy.xlsx".format(Document_path)
        df = read_data(data_file)
    print(data_file)
    img_path = "{}/img_samoyed_dev_corr_loan".format(Data_path)
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    get_loan_rule_analysis(df1=df, os_name=os_name, img_path=img_path)
    g = loan_graph(df)
    loan_report(g, os_name, img_path)


if __name__ == '__main__':
    pic_main()
    pic_main(os_name='android')
