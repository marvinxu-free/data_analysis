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
from maxent_dp.week_state import get_week_data

if __name__ == '__main__':
    path = "/flume/event_maxentid"
    tid = "ad944d96335d3e3a8bb32bb30a577fbb"
    save_path = "/tmp/marvin/58ganji_state"
    spark = SparkSession.builder.appName("suning").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_week_data(sc=sc,
                  tid=tid,
                  path=path,
                  start="20171225",
                  end="20171231",
                  save_path=save_path)

