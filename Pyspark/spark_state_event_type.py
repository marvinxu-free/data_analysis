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
from pyspark.sql.functions import count
from pyspark import StorageLevel
from maxent_dp.read_data import read_data_by_days
import re
from maxent_dp.time_utils import get_day_range
import json


def get_event_type(sc, path, start, end, save_path):
    """

    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    day_list = get_day_range(start=start, end=end)
    for day in day_list:
        data = read_data_by_days(sc=sc, path=path, start=day, end=day)
        new_save_path = "{0}/day={1}".format(save_path, day)
        print "no need save to {0}".format(new_save_path)
        data_json = data.map(json.loads).map(lambda x: transfer(x))
        # df = data_json.map(lambda x: Row(x)).toDF()
        df = spark.createDataFrame(data_json, samplingRatio=1)
        df.persist(StorageLevel.DISK_ONLY)
        type_count = df.count()
        print "event count {0}".format(type_count)
        df = df.groupby('type').agg(count('event_id').alias('type_event_num'))
        print df
        new_csv_path = "{0}_csv/day={1}".format(save_path, day)
        print "csv path is {0}".format(new_csv_path)
        df.write.mode("overwrite").option("header", "true").csv(new_csv_path)


def transfer(row):
    a = {}
    a['type'] = row['content'].get("typeclass", None)
    a['event_id'] = row['content'].get("event_id", None)
    return a


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/type_event_state"
    spark = SparkSession.builder.appName("type").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_event_type(sc=sc, path=path,
                         start="20180122", end="20180128",
                         save_path=save_path)

