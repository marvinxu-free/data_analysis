# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/20
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
玖富数据需求：
1）统计事件类型、设备数量、事件数量、疑似欺诈设备数量、疑似欺诈事件数量。
2）疑似欺诈数据举例，外加单点特征输出。
欺诈风险特征总结：

1、触发单点特征。设备机型异常、代理检测、模拟器检测（App功能）、设备破解（App功能）

2、相同的Maxent ID不同的手机品牌、型号。

3、相同的Maxent ID不同的广告标识符、国际移动装备辨识码（IMEI、IDFV）。（App功能）

4、相同的Maxent ID短时间内频繁的切换IP地址。

注：标记App功能只有在App上可用，其他功能在H5于App上均可用
"""
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


def or_filter(df_1, cond_list):
    return df_1[logical_or.reduce(cond_list)]


def and_filter(df_1, cond_list):
    return df_1[logical_and.reduce(cond_list)]


def state_all(data_dir, excel_file):
    """
    this function used to state all data and save to path
    :param data_dir:
    :param path:
    :return:
    """
    df = read_multi_csv(path=data_dir)
    df = df.sort_values(by="timestamp")
    convert_cols = df.select_dtypes(include=[np.object_]).columns.tolist() + df.select_dtypes(
        include=[np.bool_]).columns.tolist()
    df[convert_cols] = df[convert_cols].astype(str)
    # excel_file = "{0}/jiufu_statistic.xlsx".format(path)
    # excel_file = "{0}/统计报表.xlsx".format(path)
    writer = pd.ExcelWriter(excel_file)
    singal_fetures = ['is_proxy', 'ua_mismatch']
    singal_fetures_conditions = []
    for feature in singal_fetures:
        cond1 = df[feature] == 'True'
        cond2 = df[feature] == 'true'
        singal_fetures_conditions.append(cond1)
        singal_fetures_conditions.append(cond2)

    # 按天统计
    df_day = get_day_state(df=df, fraud_conditions=singal_fetures_conditions, resample_ratio='1D')
    df_day.to_excel(writer, "按天统计事件类型", index=False, engine=xlsxwriter)
    df_day_p = get_day_state(df=df, fraud_conditions=singal_fetures_conditions, resample_ratio='1D', state_type='pid')
    df_day_p.to_excel(writer, "按天统计渠道", index=False, engine=xlsxwriter)
    # 触发单点特征数据
    df_singal_point = or_filter(df, singal_fetures_conditions)
    df_singal_point["time_format"] = \
        pd.to_datetime(df_singal_point['timestamp'],
                       unit="ms",utc=True).dt.strftime("%Y/%m/%d %H:%M")
    df_singal_point.to_excel(writer, "单点特征统计", index=False, engine=xlsxwriter)
    # 设备维度统计数据
    df_ip = device_ip_anomaly(df=df, gcol='maxent_id')
    df_ip.to_excel(writer, "ip切换异常统计", index=False, engine=xlsxwriter)
    writer.save()

    # brand_path = "{0}/device_brand_anomaly.csv".format(path)
    # df_brand = device_state(df=df, gcol='maxent_id', acols=['brand'], thresholds=[1])
    # df_brand.loc[df_brand['brand_num'] == 1].to_csv(brand_path, index=False, encoding='utf-8-sig')
    # model_path = "{0}/device_model_anomaly.csv".format(path)
    # df_model = device_state(df=df, gcol='maxent_id', acols=['model'], thresholds=[2])
    # df_model.to_csv(model_path, index=False, encoding='utf-8-sig')


def device_ip_anomaly(df, gcol, time_scope=5, threshold=3):
    """
    :param df:
    :param gcol:
    :param time_scope:
    :param threshold:
    :return:
    """
    df = df.copy()

    def get_switch_ip(row):
        switch_ip_list = []
        time_stamp = list(row['timestamp'])
        ip = list(row['ip'])
        for i in xrange(len(time_stamp) - 1):
            pre_time, next_time = time_stamp[i], time_stamp[i + 1]
            pre_ip, next_ip = ip[i], ip[i + 1]
            time_delta = (next_time - pre_time) / 1000 / 60
            if pre_ip != next_ip and time_delta <= time_scope:
                switch_ip_list.extend([pre_ip, next_ip])
        ip_str = " ".join(set(switch_ip_list))
        return ip_str
    df_ip = df.groupby(gcol).apply(lambda x: get_switch_ip(x)).reset_index(name='ip')
    df_ip['ip_num'] = df_ip['ip'].apply(lambda x: len(x.split(" ")))
    df_ip = df_ip.loc[df_ip['ip_num'] >= threshold]
    return df_ip


def device_state(df, gcol, acols, thresholds):
    """
    this function used to state data based on device
    :param df:
    :param gcol:
    :param acols:
    :param thresholds:
    :return:
    """
    df = df.copy()
    df[acols] = df[acols].astype(str)

    def join_series(row):
        """
        this function used to join unique values after groupby
        :param row:
        :return:
        """
        unique_row = row.unique()
        row_str = " ".join(unique_row)
        return row_str

    df_m = df.groupby(gcol).agg({x: join_series for x in acols}).reset_index()
    for acol in acols:
        acol_num = "{0}_num".format(acol)
        df_m[acol_num] = df_m[acol].apply(lambda x: len(x.split(" ")))
    filter_conditions = []
    num = re.compile('.*_num$')
    num_match = np.vectorize(lambda x: bool(num.match(x)))
    num_cols = df_m.columns.values[num_match(df_m.columns.values)]
    for num_col, th in zip(num_cols, thresholds):
        condtion = df_m[num_col] >= th
        filter_conditions.append(condtion)

    df_m = or_filter(df_m, filter_conditions)

    return df_m


def get_day_state(df, fraud_conditions, resample_ratio, state_type='type'):
    """
    this function used to get a report sheet for each day and type
    for event_id num, maxent_id num, fraud event_id num, fraud maxent_id num
    :param df:
    :param fraud_conditions:
    :return:
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.set_index("timestamp")
    groups = [state_type, pd.TimeGrouper(resample_ratio)]
    df_nor = df.groupby(groups).agg({'maxent_id': 'nunique',
                                     'event_id':'nunique'}).reset_index()

    df_nor = df_nor.rename(columns={'maxent_id':'device_num',
                             'event_id':'event_num'})
    df_fraud = or_filter(df, fraud_conditions)
    df_fraud= df_fraud.groupby(groups).agg({'maxent_id': 'nunique',
                                     'event_id':'nunique'}).reset_index()
    df_fraud = df_fraud.rename(columns={'maxent_id':'fraund_device_num',
                             'event_id':'fraud_event_num'})
    df_day = df_fraud.merge(df_nor, on=[state_type,'timestamp'])
    df_day = df_day.sort_values(by="timestamp")
    df_day["day"] = pd.to_datetime(df_day['timestamp'], unit="ms",
                                   utc=True).dt.strftime("%Y/%m/%d")
    df_day = df_day.drop('timestamp', axis=1)

    return df_day


if __name__ == '__main__':
    # data_dir = "/Users/chaoxu/code/local-spark/Data/jiufu/jiufu_state_csv"
    # data_dir = "/Users/chaoxu/code/local-spark/Data/suning/suning_state_csv"
    # data_dir = "/Users/chaoxu/code/local-spark/Data/suning/suning_state_csv"
    # state_all(data_dir="/Users/chaoxu/code/local-spark/Data/week_state/58ganji_state_csv",
    #           excel_file="/Users/chaoxu/code/local-spark/Document/week_state/58赶集.xlsx")
    state_all(data_dir="/Users/chaoxu/code/local-spark/Data/week_state/aitouzi_state_csv",
              excel_file="/Users/chaoxu/code/local-spark/Document/week_state/爱投资.xlsx")
