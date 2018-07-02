# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file is used to get uniq imei from beta during 0801-0820
which label 1 : label 0 = 1: 10
"""

from pyspark.sql import SparkSession
import json
from pyspark import StorageLevel
import calendar
import datetime
import time
import re


def readData(path, sc, month):
    print("begin to read month {0}".format(month))
    if month < 10:
        print("begin to read month {0}".format(month))
        path = path +"/day=20170{0}*".format(month)
    else:
        print("begin to read month {0}".format(month))
        path = path +"/day=2017{0}*".format(month)
    data = sc.textFile(path)
    return data


def get_month_scope(year=2017,month=range(6,13), mode='str'):
    """
    this function used to get month begin day and last day
    :param year:
    :param month:
    :return:
    """
    scope = []
    for mon in month:
        _, num_days = calendar.monthrange(year=year, month=mon)
        first_day = datetime.date(year=year, month=mon, day=1)
        last_day = datetime.date(year=year, month=mon, day=num_days)
        first_day_tupple = datetime.datetime.combine(first_day, datetime.time.min)
        last_day_tupple = datetime.datetime.combine(last_day, datetime.time.max)
        if mode == 'str':
            first_day_str = first_day_tupple.strftime("%Y%m%d")
            last_day_str = last_day_tupple.strftime("%Y%m%d")
            scope.append((first_day_str, last_day_str))
        else:
            first_day_timestamp = time.mktime(first_day_tupple.timetuple()) * 1000.0
            last_day_timestamp = time.mktime(last_day_tupple.timetuple()) * 1000.0
            scope.append((first_day_timestamp, last_day_timestamp))
    return scope


def check_data(path, sc):
    """
    this function used to read file recursive and give report for each month
    :param path:
    :param sc:
    :return: nothing
    """

    # day_scope = get_month_scope()
    for mon in range(10, 13):
        data = readData(path, sc=sc, month=mon)
        data = data.filter(lambda x: ("8enu94eqhvol15pe71als1ojbo0fe147" in x) or ("qcthzpb1u4ta0kgsx83lya6ux43thbyi" in x))
        data.persist(StorageLevel.DISK_ONLY)
        print("all data in tids is {0}".format(data.count()))
        data_pro_num = data.filter(lambda x: "8enu94eqhvol15pe71als1ojbo0fe147" in x ).count()
        data_test_num = data.filter(lambda x: "qcthzpb1u4ta0kgsx83lya6ux43thbyi" in x ).count()
        print("baoyin nums from month {0}:\n\tproduct num: {1}\n\ttest num: {2}".format(mon, data_pro_num, data_test_num))

if __name__ == "__main__":
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/resolution"
    spark = SparkSession.builder.appName("resolution").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    df = check_data(path=path, sc=sc)

