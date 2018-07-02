# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/13
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to generate some data for echart
"""
from __future__ import division, print_function
import numpy as np


def get_city_custer(df, ix=5, city_num=30, json_path=None):
    """
    this function used to create data for echart
    :param df:
    :param json_path:
    :param ix:
    :param city_num:
    :return:
    """
    df = df.groupby(['city', 'mobile']).agg({"ckid": "nunique"}).reset_index()
    df = df.rename(columns={"ckid" : "value", "city":"name"})
    df = df.loc[df['name'] != 'UNKNOWN'].sort_values(by="value", ascending=False).iloc[0:city_num-1]
    df_ratio = df.rename(columns={"value":"value_all"})
    thresholds = np.arange(df['value'].describe().ix[ix], df['value'].max()+1,1)
    for threshold in thresholds:
        print("同一个设备对应ckid大于{0}的城市".format(threshold))
        df_th = df.loc[df['value'] >= threshold].reset_index(drop=True)
        df_ratio_th = df_ratio.merge(df_th.drop("name",axis=1),on='mobile')
        df_th['name'] = df_th.apply(lambda x :x['name'].decode('utf-8')[:-1], axis=1)
        df_th = df_th[['name','value']]
        df_ratio_th['ratio'] = df_ratio_th['value'] / df_ratio_th['value_all']
        df_ratio_th['name'] = df_ratio_th.apply(lambda x: x['name'].decode('utf-8')[:-1], axis=1)
        df_ratio_th = df_ratio_th[['name','ratio']]
        path1 = json_path + "/city_{0}_num.json".format(threshold)
        path2 = json_path + "/city_{0}_ratio.json".format(threshold)
        df_th.to_json(path1,force_ascii=False,orient='records', lines=True)
        df_ratio_th.to_json(path2, force_ascii=False, orient='records',lines=True)


def get_city_cluster_id(df, ix=5, city_num=30, json_path=None):
    """
    this function used to get city cluster in ID feature analysis
    :param df:
    :param ix:
    :param city_num:
    :param json_path:
    :return:
    """
    df = df.loc[df['city'] != 'UNKNOWN']
    df_m = df.groupby('mobile').agg({"ckid": "nunique"}).reset_index()
    df_m = df_m.rename(columns={"ckid": "ckid_value"})
    df_c = df.groupby("city").agg({"mobile": "nunique"}).reset_index()
    df_c = df_c.rename(columns={"mobile": "mobile_value"})
    thresholds = np.arange(df_m['ckid_value'].describe().ix[ix], df_m['ckid_value'].max()+1, 1)
    for threshold in thresholds:
        df_mt = df_m.loc[df_m['ckid_value'] >= threshold]
        if not df_mt.empty:
            df_th = df.merge(df_mt, on='mobile')
            df_th_c = df_th.groupby("city").agg({"mobile": "nunique"}).reset_index()
            df_th_c = df_c.merge(df_th_c, on='city')
            df_th_c = df_th_c.rename(columns={"mobile": "value"})
            df_th_c['ratio'] = df_th_c['value'] / df_th_c['mobile_value']
            # df_th = df_th.groupby("mobile").agg({"city": "nunique"}).reset_index()
            # df_th = df_th.rename(columns={"city": "value"})
            # df_th = df.drop_duplicates(['mobile','city']).reset_index()\
            #     .merge(df_th, on='mobile').merge(df_mt.drop("value",axis=1), on='mobile')
            path1 = json_path + "/id_cluster_city_ckid>{0}_ratio.json".format(threshold)
            path2 = json_path + "/id_cluster_city_ckid>{0}_num.json".format(threshold)
            df_th_c[['city','value']].to_json(path2,force_ascii=False,orient='records', lines=True)
            df_th_c[['city','ratio']].to_json(path1,force_ascii=False,orient='records', lines=True)

