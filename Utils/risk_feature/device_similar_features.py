# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to get similar fearture datas based on excel
"""


# def get_active_distribution(df, similar_threshold, active_fileds):
#     """
#
#     :param df:
#     :param similar_threshold:
#     :param active_filed:主动式设备指纹id区域， 对于H5是ckid，
#     对于Android是mcid, aid, imei+mac, 三者匹配上一个就行
#     :return:
#     """


def get_active_distribution(df, similar_threshold, active_filed):
    """

    :param df:
    :param similar_threshold:
    :param active_filed:
    :return:
    """
    df = df.loc[df['similar_score'] >= similar_threshold]
