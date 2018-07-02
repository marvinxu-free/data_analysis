# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to generate similar pictures
"""
from Utils.risk_feature.read_device_similar import get_similar_data, merge_df


def pic_similar(df, fsimilar, img_path):
    """
    this function used to integrate
    all picture about similar analysis
    :param df:
    :param fsimilar:
    :param img_path:
    :return:
    """
    df_s = get_similar_data(fsimilar)
    df = merge_df(df1=df, df2=df_s, col_left='maxent_id')