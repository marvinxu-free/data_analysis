# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to read
"""
import pandas as pd
from Utils.common.custerReadFile import custom_open
import json


def get_similar_data(fname):
    """
    this function used to read similar data
    and output a data frame
    :param fname:
    :return:
    """
    with custom_open(fname) as f:
        a = []
        for similar_json in f:
            doc = json.loads(similar_json)
            similar_data = doc.get("similarDevice")
            if len(similar_data) > 0 :
                similar_max_score = max(map(lambda x: x['score'], similar_data))
                a.append({'device_id': doc['device']['device_id'],
                          'similar_score':similar_max_score})
        df = pd.DataFrame(a)
        return df


def merge_df(df1, df2, col_left, col_right='device_id'):
    """
    this file used to merge similar data to df1 based on cols
    :param df1:
    :param df2:
    :param col_left:
    :param col_right:
    :return:
    """
    df = pd.merge(df1, df2, left_on=col_left, right_on=col_right)
    return df


