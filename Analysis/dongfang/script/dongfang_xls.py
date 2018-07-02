# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/5/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本文件主要根据唐亘的excel，分析生成对应的数据
分析的数据范围是从20180501-20180515
统计了
"""
from __future__ import print_function, division
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import pandas as pd
import xlsxwriter
from Params.path_params import Document_path, Data_path
from Utils.common.custerReadFile import read_multi_csv
from pandas import Grouper


def get_df(data_path):
    """
    读取csv数据
    :param data_path:
    :return:
    """
    df = read_multi_csv(data_path)
    return df


def get_sheet1(df):
    """
    获取统计结果这张表
    :param df:
    :return:
    """
    df1 = df.groupby('sub_pid_seller', as_index=False).agg(
        {'score': 'mean', 'event_id': 'nunique', 'maxentID': 'nunique'})
    df1 = df1.rename(columns={'sub_pid_seller': u'渠道', 'score': u'平均欺诈得分', 'event_id': u'事件数', 'maxentID': u'去重设备数'})
    df2 = df.loc[df.score >= 90].groupby('sub_pid_seller', as_index=False).agg({'event_id': 'nunique'})
    df2 = df2.rename(columns={'sub_pid_seller': u'渠道', 'event_id': u'疑似欺诈事件数'})
    df = df1.merge(df2, on=u'渠道', how='left')
    df[u'欺诈比例'] = df[u'疑似欺诈事件数'] / df[u'事件数']
    return df


def get_sheet2(df):
    """
    获取网段短时间大量聚集的统计
    :param df:
    :return:
    """
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['hour'] = df.time.dt.hour
    df['day'] = df['time'].dt.strftime("%Y/%m/%d")
    df1 = df.groupby(['ipSeg24', 'day', 'hour']).agg({'score': 'mean', 'event_id': 'count', 'ipSeg24_1h_value': 'max'}) \
        .sort_values(by=['ipSeg24_1h_value', 'event_id', 'score'], ascending=[False, False, False])
    bad_ip = df1['ipSeg24_1h_value'].idxmax()
    print('bad ip is {0}'.format(bad_ip[0]))
    df2 = df1.loc[(bad_ip[0],),].sort_index(level=['day', 'hour'])
    df3 = df.loc[
        (df['ipSeg24'] == bad_ip[0]) & (df['day'] == bad_ip[1]) &
        (df['hour'] == bad_ip[2])][["ipSeg24_1h_value", 'hour', 'time', 'timestamp']]
    df4 = df3.rename(columns={'ipSeg24_1h_value': 'count'}).sort_values(by='timestamp')
    df4['time'] = df4['time'].dt.strftime("%Y/%m/%d %H:%M")
    return df2, df4


def get_sheet3(df):
    """
    本函数获取sheet3的数据
    :param df:
    :return:
    """
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['hour'] = df.time.dt.hour
    df['day'] = df['time'].dt.strftime("%Y/%m/%d")
    df1 = df.groupby(['maxentID', 'day', 'hour'], as_index=False).agg({'event_id': 'count'})
    bad_dev = df1.loc[df1['event_id'] == df1['event_id'].max(), ['maxentID', 'day', 'hour']].iloc[0]
    print('bad device is {0}, with num of {1}'.format(bad_dev['maxentID'], df1.shape[0]))
    df3 = df.loc[
        (df['maxentID'] == bad_dev['maxentID']) & (df['day'] == bad_dev['day']) & (df['hour'] == bad_dev['hour'])]
    df3 = df3[['ipGeo_5m_value', 'maxentID', 'time', 'timestamp']].sort_values(by='timestamp')
    df3['time'] = df3['time'].dt.strftime("%Y/%m/%d %H:%M")
    return df3


def get_sheet4(df):
    """
    本函数获取sheet4的数据
    :param df:
    :return:
    """
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['hour'] = df.time.dt.hour
    df['day'] = df['time'].dt.strftime("%Y/%m/%d")
    df = df.loc[(df.day == '2018/05/13') & (df.city != 'UNKNOWN')]
    df1 = df.groupby(['city', 'hour'], as_index=False).agg({'event_id': 'count'}).rename(columns={'event_id': u'事件数'})
    df2 = df.loc[df.isSimulator_value == True].groupby(['city', 'hour'], as_index=False).agg(
        {'event_id': 'count'}).rename(columns={'event_id': u'使用模拟器数量'})
    df3 = df1.merge(df2, on=['city', 'hour'], how='right').rename(columns={'city': u'城市', 'hour': u'时间'})
    df3[u'使用模拟器的比例'] = df3[u'使用模拟器数量'] / df3[u'事件数']
    # df3 = df3.loc[df3.groupby([u'城市'])[u'使用模拟器数量'].idxmax()].sort_values(by=u'使用模拟器的比例', ascending=False)
    df3 = df3.loc[df3[u'使用模拟器数量'] >=5]
    return df3


def xls_main(data_path, xls_file):
    """
    主函数
    :param data_path:
    :param xls_file:
    :return:
    """
    writer = pd.ExcelWriter(xls_file)
    df = get_df(data_path)
    print('read data done')
    sht1_df = get_sheet1(df)
    sht1_df.to_excel(writer, u"统计结果", index=False, engine=xlsxwriter)
    sht2_df_s, sht2_df_b = get_sheet2(df)
    sht2_df_s.to_excel(writer, u"网段上，短时间大量聚集", engine=xlsxwriter, startrow=1, startcol=1)
    sht2_df_b.to_excel(writer, u"网段上，短时间大量聚集", index=False, engine=xlsxwriter, startrow=16, startcol=1)
    sht3_df = get_sheet3(df)
    sht3_df.to_excel(writer, u"同一设备反复操作", index=False, engine=xlsxwriter, startrow=9, startcol=0)
    sht4_df = get_sheet4(df)
    sht4_df.to_excel(writer, u"同一地理位置，大量使用模拟器", index=False, engine=xlsxwriter, startrow=5, startcol=0)
    writer.save()
    print('done')


if __name__ == '__main__':
    data_dir = "{0}/dongfang/dongfang_csv".format(Data_path)
    excel_file = "{0}/dongfang/东方头条刷量统计.xlsx".format(Document_path)
    xls_main(data_dir, excel_file)
