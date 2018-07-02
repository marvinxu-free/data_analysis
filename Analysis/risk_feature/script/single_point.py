# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/18
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to get pic from signal point features from excel
"""
import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000
from Utils.risk_feature.singal_point_features import get_cluster_data
from Pic.pic_scatter import pic_scatter_bubble_2d


def pic_singal_point(df, img_path):
    """
    get pic from df and save to img_path
    :param df:
    :param img_path:
    :return:
    """
    # ----单点特征-----
    df['ip_c'] = df['ip'].apply(lambda x : ".".join(x.split(".")[:2]))
    df_proxy_ckid_ip = get_cluster_data(df=df, fcol='is_proxy', gcol='ip_c', acol='ckid',path=img_path)
    pic_scatter_bubble_2d(
        x=df_proxy_ckid_ip['ckid'].values,
        y=df_proxy_ckid_ip['ratio'].values,
        text=df_proxy_ckid_ip['ip_c'].values,
        xlabel="使用代理的设备指纹ID数量", ylabel="同一ip字段使用代理的设备指纹ID比例",
        # title="使用代理的ip聚集度",
        fname=img_path + "/is_proxy_ip_cluster.png")
    df_uamis_ckid_ip = get_cluster_data(df=df, fcol='ua_mismatch', gcol='ip_c', acol='ckid',path=img_path)
    pic_scatter_bubble_2d(
        x=df_uamis_ckid_ip['ckid'].values,
        y=df_uamis_ckid_ip['ratio'].values,
        text=df_uamis_ckid_ip['ip_c'].values,
        xlabel="设备异常的设备指纹ID数量", ylabel="同一ip字段设备异常的设备指纹ID比例",
        # title="设备异常的ip聚集度",
        fname=img_path + "/is_uamis_ip_cluster.png")
    df_proxy_ckid_city = get_cluster_data(df=df, fcol='is_proxy', gcol='city', acol='ckid',path=img_path)
    pic_scatter_bubble_2d(
        x=df_proxy_ckid_city['ckid'].values,
        y=df_proxy_ckid_city['ratio'].values,
        text=df_proxy_ckid_city['city'].values,
        xlabel="使用代理的设备指纹ID数量", ylabel="同一城市使用代理的设备指纹ID比例",
        # title="使用代理的城市聚集度",
        fname=img_path + "/is_proxy_city_cluster.png")
    df_uamis_ckid_city = get_cluster_data(df=df, fcol='ua_mismatch', gcol='city', acol='ckid',path=img_path)
    pic_scatter_bubble_2d(
        x=df_uamis_ckid_city['ckid'].values,
        y=df_uamis_ckid_city['ratio'].values,
        text=df_uamis_ckid_city['city'].values,
        xlabel="设备异常的设备指纹ID数量", ylabel="同一城市设备异常的设备指纹ID比例",
        # title="设备异常的ip聚集度",
        fname=img_path + "/is_uamis_city_cluster.png")
