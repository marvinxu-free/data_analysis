# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/4/3
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本文件用来给万达产生需要的excel文件
"""

from __future__ import print_function, division
import sys

reload(sys)
sys.setdefaultencoding('utf8')
from Utils.common.custerReadFile import read_multi_csv
import pandas as pd
import xlsxwriter
from Params.path_params import Document_path, Data_path
from copy import deepcopy
import os
import errno
import numpy as np
from collections import OrderedDict
from scipy.stats import beta

common_map_cols = {
    u'操作系统': 'os',
    u'手机品牌': 'brand',
    u'手机型号': 'model',
    u'上网方式': 'ns',
    u'渠道号': 'pid',
    u'子渠道号': 'sub_pid',
    u'事件会话号': 'session_id',
    u'app应用版本': 'appversion',
    u'sdk版本': 'sdkversion',
    u'事件唯一标识符': 'event_id',
    u'用户名': 'userid',
    u'激活时的消息类型': '__tact',
    u'消息类型': 'typeclass',
    u'方便客户自定义内容': 'customMsg',
    u'是否root': 'is_cracked',
    u'猛犸提供的ID账户(与TID一致)': 'tid',
    u'时间戳': 'timestamp',
    u'猛犸ID(设备ID)': 'maxent_id',
    u'分辨率': 'resolution',
    u'运营商': 'cr',
    u'设备机型异常': 'ua_mismatch',
    u'代理检查异常': 'is_proxy',
    u'模拟器检测异常': 'is_simulator',
    u'设备破解': 'is_cracked',
    u'国家': 'country',
    u'国家代码': 'country_code',
    u'省': 'province',
    u'市': 'city',
    u'设备类型': 'device',
    u'campaign_id': 'campaign_id',
    u'UA信息': 'user_agent'
}

ios_map_cols = {
    u'广告标识符': 'idfa',
    u'Cookie ID': 'ckid',
}

android_map_cols = {
    u'广告标识符': 'aid',
    u'国际移动装备标识符': 'imei',
    u'物理网卡地址': 'mac',
    u'Cookie ID': 'ckid',
}


def csv2xls(csv_dir, xls_name, os_name):
    """
    this file used to read csv files in csv_dir and output a xls
    :param csv_dir:
    :param xls_name:
    :return:
    """
    map_cols = deepcopy(common_map_cols)
    if os_name == 'ios':
        map_cols.update(ios_map_cols)
    else:
        map_cols.update(android_map_cols)
    rename_cols = {}

    for k, v in map_cols.iteritems():
        rename_cols[v] = k

    df = read_multi_csv(csv_dir)
    df[u"时间"] = \
        pd.to_datetime(df['timestamp'],
                       unit="ms", utc=True).dt.strftime("%Y/%m/%d %H:%M")
    df = df.rename(columns=rename_cols)
    writer = pd.ExcelWriter(xls_name)
    df.to_excel(writer, u"{0}需求字段".format(os_name), index=False, engine=xlsxwriter)
    writer.save()


if __name__ == '__main__':
    csv_dir = "{0}/liuqu/android_csv".format(Data_path)
    xls_name = "{0}/liuqu/android.xlsx".format(Data_path)
    csv2xls(csv_dir=csv_dir, xls_name=xls_name, os_name='android')
