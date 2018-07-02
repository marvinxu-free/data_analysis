# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/5
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to get time range for special use,
such as read less files based on especially need
"""
import datetime


def get_day_range(start, end, delta=1):
    """
    this file used to get day rang from star to end,
    end is included;
    start and end should be format as %Y%m%d which is same as hdfs
    :param start:
    :param end:
    :param delta:
    :return:
    """
    step = datetime.timedelta(days=delta)
    start_date = datetime.datetime.strptime(start, "%Y%m%d")
    end_date = datetime.datetime.strptime(end, "%Y%m%d")
    day_range = []
    while start_date <= end_date:
        day_range.append(start_date.date())
        start_date += step
    day_range_str = map(lambda x: x.strftime("%Y%m%d"), day_range)
    return day_range_str


