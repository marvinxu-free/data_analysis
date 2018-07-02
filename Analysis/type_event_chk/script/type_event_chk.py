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
import sys

reload(sys)
sys.setdefaultencoding('utf8')
from Utils.common.custerReadFile import read_multi_csv
import numpy as np
import re
from numpy import logical_or, logical_and
import pandas as pd
import xlsxwriter
from Params.path_params import Data_path, Document_path
from Utils.common.time_utils import get_day_range


def chk_type(f1, start, end, sfile):
    """

    :param f1:
    :param time_scope:
    :param sfile:
    :return:
    """
    writer = pd.ExcelWriter(sfile)
    day_list = get_day_range(start, end)
    for day in day_list:
        csv_path = "{0}/day={1}".format(f1, day)
        df = read_multi_csv(csv_path)
        df = df.sort_values(by='type_event_num', ascending=False)
        df.to_excel(writer, "{0}".format(day), index=False, engine=xlsxwriter)
    writer.save()


if __name__ == '__main__':
    data_dir = "{0}/type_event_state_csv".format(Data_path)
    excel_file = "{0}/type_check/事件类型统计.xlsx".format(Document_path)
    chk_type(f1=data_dir, start='20180122',
             end='20180128', sfile=excel_file)
