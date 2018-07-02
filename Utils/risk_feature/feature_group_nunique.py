# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to get data frame from group and agg with nunique
"""


def feature_group_nunique(df, gcol, acol):
    """
    this function used to get new df from groupby gcol and agg from acol
    :param df:
    :param gcol:
    :param acol:
    :return:
    """
    if isinstance(acol, list) or len(acol) >0:
        df = df.groupby(gcol).agg(dict(map(lambda x:(x,'nunique'), acol)))
    return df