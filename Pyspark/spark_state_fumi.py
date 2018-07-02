# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/5
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
统计下 福米金融 最近的上报量和查询量，thanks
产品：ID系统
环境：生成环境
tid：5g0hau6ige6ndtwx6fg9a9gyxbhcgjg4
"""

from pyspark.sql import SparkSession
from maxent_dp.hive import get_tid_upload
from maxent_dp.read_data import read_data_by_days
from maxent_dp.time_utils import get_day_range


def get_total_num(sc, path, tid, start, end):
    """
    this function used to state tid event nums in path from daylist
    :param sc:
    :param path:
    :param start:
    :param end:
    :return:
    """
    day_list = get_day_range(start=start, end=end)
    for day in day_list:
        path_str = "{0}/day={1}".format(path, day)
        print("read data path is {0}".format(path_str))
        data = sc.textFile(path_str)
        data_cnt = data.filter(lambda x: tid in x).count()
        print("tid:{0} in day {1} num is {2}".format(tid, day, data_cnt))


if __name__ == "__main__":
    path = "/flume/event_maxentid"
    # save_path = "/tmp/marvin/resolution"
    tid = "5g0hau6ige6ndtwx6fg9a9gyxbhcgjg4"
    spark = SparkSession.builder.appName("fumi").enableHiveSupport().getOrCreate()
    # upload_cnt = get_tid_upload(spark=spark, tid=tid, start="20171128", end="20171204")
    upload_cnt = 52591
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_total_num(sc=sc, path=path, tid=tid, start="20171128", end="20171204")


