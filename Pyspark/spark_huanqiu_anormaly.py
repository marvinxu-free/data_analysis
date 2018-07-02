# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
根据brand_anornaly.csv里面的数据获取对应的异常事件
"""
from pyspark.sql import SparkSession, Row
from maxent_dp.read_data import read_data_by_days
import re
import os
import json


def get_huanqiu_heika(sc, path, start, end, save_path):
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
        data_json = data.map(json.loads).map(lambda x: transfer(x))
        df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)
    except Exception as e:
        print "error: {0}".format(e)
        data = read_data_by_days(sc=sc, path=path, start=start, end=end)
        data = data.filter(lambda x: "b1235335be1b36748fbce2def4e281b9" in x)
        data.saveAsTextFile(path=save_path)
        data_json = data.map(json.loads).map(lambda x: transfer(x))
        df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)


def transfer(row):
    a = {}
    r = re.compile(".*anomaly$")
    a['timestamp'] = row['content'].get("timestamp")
    a['maxent_id'] = row['content'].get("maxent_id", None)
    a['typeclass'] = row['content'].get("typeclass", None)
    a['event_id'] = row['content'].get("event_id", None)
    a['tid'] = row['content'].get("tid", None)
    # anomaly_dict = {x: y for x, y in row['features'].iteritems() if "anomaly" in str(x)}
    anomaly_dict = {}
    for x, y in row['features'].iteritems():
        if "anomaly" in str(x):
            anomaly_dict[x] = y
    a.update(anomaly_dict)
    return a

if __name__ == '__main__':
    path = "/flume/original_scorer_input_data"
    save_path = "/tmp/marvin/huanqiu_heika_anormaly"
    spark = SparkSession.builder.appName("heika").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_huanqiu_heika(sc=sc, path=path,
                      start="20180226", end="20180227",
                      save_path=save_path)
