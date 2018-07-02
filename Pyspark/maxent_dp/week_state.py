# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/3
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pyspark.sql import Row
from read_data import read_data_by_days
import json


def get_week_data(sc, tid, path, start, end, save_path):
    """

    :param sc:
    :param tid:
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
        df = data_json.toDF()
        # df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)
    except Exception:
        print "firsr get data"
        data = read_data_by_days(sc=sc, path=path, start=start, end=end)
        data = data.filter(lambda x: tid in x)
        data.saveAsTextFile(path=save_path)
        data_json = data.map(json.loads).map(lambda x: transfer(x))
        df = data_json.toDF()
        # df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)


def transfer(row):
    a = {}
    a['timestamp'] = row['content'].get("timestamp")
    a['maxent_id'] = row['content'].get("maxent_id", None)
    a['type'] = row['content'].get("type", None)
    a['pid'] = row['content'].get("pid", None)
    a['ip'] = row['content'].get("ip", None)
    a['event_id'] = row['content'].get("event_id", None)
    a['country'] = row['content']['ip_geo'].get("country", None)
    a['country_code'] = row['content']['ip_geo'].get("country_code", None)
    a['province'] = row['content']['ip_geo'].get("province", None)
    a['city'] = row['content']['ip_geo'].get("city", None)
    a['is_cracked'] = row['content']["features"].get("is_cracked", "UNKNOWN")
    a['ua_mismatch'] = row['content']["features"].get("ua_mismatch", "UNKNOWN")
    a['is_proxy'] = row['content']["features"].get("is_proxy", "UNKNOWN")
    a['is_simulator'] = row['content']["features"].get("is_simulator", "UNKNOWN")
    a.update(row['content']["did"])
    a.update(row['content']["device_model_json"])
    return a