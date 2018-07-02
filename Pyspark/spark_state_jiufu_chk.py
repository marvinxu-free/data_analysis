# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to give a stastic report for huanbei based on jiufu report
add dfp detail
"""
from pyspark.sql import SparkSession, Row
from pyspark import StorageLevel
from maxent_dp.read_data import read_data_by_days
import re
from maxent_dp.time_utils import get_day_range
import json


def get_huanbei_data_dfp(sc, path, start, end, save_path):
    """

    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    csv_path = "{0}_csv".format(save_path)
    try:
        data = sc.textFile(save_path)
        print "data already exist!"
        data_json = data.map(json.loads).map(lambda x: transfer(x))
        # df = data_json.map(lambda x: Row(x)).toDF()
        df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)
    except Exception:
        data = read_data_by_days(sc=sc, path=path, start=start, end=end)
        data = data.filter(lambda x: "6016851634c8359b9d272d526ba6034f" in x)
        data.saveAsTextFile(path=save_path)
        data_json = data.map(json.loads).map(lambda x: transfer(x))
        df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)

def transfer(row):
    a = {}
    a['timestamp'] = row['content'].get("timestamp")
    a['maxent_id'] = row['content'].get("maxent_id", None)
    a['type'] = row['content'].get("type", None)
    a['event_id'] = row['content'].get("event_id", None)
    return a


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/jiufu_chk"
    spark = SparkSession.builder.appName("jiufu").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_huanbei_data_dfp(sc=sc, path=path,
                         start="20180109", end="20180120",
                         save_path=save_path)

