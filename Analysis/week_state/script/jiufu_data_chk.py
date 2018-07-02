# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/23
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
检查某一时间范围内玖富的数据
"""

from __future__ import print_function, division
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')
from Utils.common.custerReadFile import read_multi_csv
import numpy as np
import re
from numpy import logical_or, logical_and
import pandas as pd
import xlsxwriter
from Params.path_params import Data_path, Document_path


def chk_data(f1, time_scope, sfile):
    """

    :param f1:
    :param time_scope:
    :param sfile:
    :return:
    """
    df = read_multi_csv(f1)
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.loc[(df['time'] >= time_scope[0]) & (df['time'] <= time_scope[1])]
    df = df.drop('timestamp', axis=1).reset_index(drop=True)
    df['time'] = df['time'].dt.strftime("%Y/%m/%d %H:%M")
    df = df.sort_values(by='time')
    writer = pd.ExcelWriter(sfile)
    df.to_excel(writer, "数据全集", index=False, engine=xlsxwriter)


if __name__ == '__main__':
    data_dir = "{0}/jiufu_chk_csv".format(Data_path)
    excel_file = "{0}/jiufu_chk_csv/玖富数据.xlsx".format(Document_path)
    chk_data(data_dir, ['2018-01-19 03:00:00', '2018-01-20 03:00:00'], excel_file)
