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
from maxent_dp.read_data import readData


# def readData(path, spark, filter_str="""day == 20171111 and hour=12"""):
#     """
#
#     :param path:
#     :param spark:
#     :param filter_str:
#     :return:
#     """
#     data = spark.read.option("mergeSchema","true").json(path)
#     data = data.filter(filter_str)
#     data = data.toJSON()
#     data = data.map(json.loads)
#     data = data.repartition(100)
#     data.persist(StorageLevel.DISK_ONLY)
#     return data


def transData(record):
    a = {}
    a["resolution"] = record["content"]["resolution"]
    a["event_type"] = record["content"]["typeclass"]
    a["device_model"] = record["content"]["device_model_json"]['model']
    a["os"] = record["content"]["os"]
    a["tid"] = record["content"]["tid"]
    return a


def get_data(df):
    data = df.filter(lambda x:
                         (x['content']['tid'] == '15cd698f13ec3769aba039bf013f2803') and
                         (x['content']['resolution'] == "375.000000,667.000000,3.000000") and
                         (x['content']['os'] == 'ios'))
    data.persist(StorageLevel.DISK_ONLY)
    data_cnt = data.count()
    print("get all resolution data num is {0}".format(data_cnt))
    return data


if __name__ == "__main__":
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/resolution_chk"
    spark = SparkSession.builder.appName("resolution").enableHiveSupport().getOrCreate()
    df = readData(path=path, spark=spark, filter_str="""day == 20171111""")
    data = get_data(df=df)
    data.coalesce(1).saveAsTextFile(save_path)

