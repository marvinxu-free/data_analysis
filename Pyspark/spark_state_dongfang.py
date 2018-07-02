# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本文件主要用于产生东方头条的统计报告，
"""
from pyspark.sql import SparkSession, Row
from pyspark import StorageLevel
from maxent_dp.read_data import read_data_by_days
from maxent_dp.hdfs_shell import chk_dir_exist
from maxent_dp.col_map import common_map_cols
import re
import json
import numpy as np
from copy import deepcopy
import pytz
from datetime import datetime


def getItem(data):
    """
    """
    a = {}
    content = data.get("content", {})
    features = data.get("features", {})
    a["tick"] = content.get("tick", None)
    a["event_id"] = content.get("event_id", None)
    a["type"] = content.get("type", None)
    a["pid"] = content.get("pid", None)
    a["sub_pid"] = content.get("sub_pid", None)
    a["sub_pid_seller"] = content.get("message", {}).get("__seller_user_id", None)
    a["tcpts"] = content.get("tcpts", None)
    a["window"] = content.get("tcp_initial_window", None)
    a["timestamp"] = content.get("timestamp", None)
    # tz = pytz.timezone("Asia/Shanghai")
    # dt = datetime.fromtimestamp(float(a["timestamp"]) / 1000, tz)
    # a["day"] = dt.strftime("%Y-%m-%d")
    # a["hour"] = dt.strftime("%H")
    a["maxentID"] = content.get("maxent_id", None)
    a["ipSeg24"] = ".".join(content["ip"].split(".")[:-1])
    a["city"] = content.get("ip_geo", {}).get("city", None)
    a["ipGeo_5m_value"] = features.get("ipGeo.5m.value")
    a["proxyIP_value"] = features.get("proxyIP.value", "false") == "true"
    a["proxyIP_anomaly"] = float(features.get("proxyIP.anomaly", "1"))
    a["isSimulator_value"] = features.get("uaMismatch.value", "false") == "true"
    # a["isSimulator_value"] = a["isSimulator_value"] | (features.get("isSimulator.value", "false") == "true")
    a["isSimulator_anomaly"] = float(features.get("uaMismatch.anomaly", "1")) \
                               + float(features.get("isSimulator.anomaly", "1")) - 1
    if a["isSimulator_value"]:
        a["isSimulator_anomaly"] += np.random.random() * 1 + 2
    else:
        pass
    a["score"] = float(np.log(a["proxyIP_anomaly"]) + np.log(a["isSimulator_anomaly"]))
    for i in features:
        if "anomaly" in i and i not in ["isSimulator.anomaly", "uaMismatch.anomaly", "proxyIP.anomaly",
                                        "ipSeg24.7d.anomaly", "shopID.1h.anomaly", "shopID.6h.anomaly",
                                        "shopID.1d.anomaly",
                                        "shopID.7d.anomaly"]:
            a["score"] += float(np.log(features[i]))
    a["score"] = float(scoreTrans(a["score"]))
    a["reason"] = ""
    return a


def scoreTrans(score):
    """
    """
    if score <= 0:
        return 0
    low = 10.
    high = 20.
    tmp = (1 - low / score) / (1 - low / high) * (2.2 - 0.85) + 0.85
    return 100 / (np.exp(-1 * tmp) + 1)


def get_dongfang_data_dfp(sc, path, start, end, save_path):
    """

    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    csv_path = "{0}/dongfang_csv".format(save_path)
    tmp_data_path = "{0}/dongdang".format(save_path)

    if chk_dir_exist(tmp_data_path):
        data = sc.textFile(tmp_data_path)
        data_json = data.map(json.loads).map(getItem)
        data_json.persist(StorageLevel.DISK_ONLY)
        data_num = data_json.count()
        print "data num is {0}".format(data_num)
        df = data_json.toDF()
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").option("nullValue", " ").csv(csv_path)
    else:
        data = read_data_by_days(sc=sc, path=path, start=start, end=end)
        data = data.filter(lambda x: "d51e44a3684930dfb458a7f1853ef2fc" in x and (
                '"type":"createaccount"' in x or '"type":"transaction"' in x))
        data.persist(StorageLevel.DISK_ONLY)
        data.saveAsTextFile(path=tmp_data_path)
        data_json = data.map(json.loads).map(getItem)
        df = data_json.toDF()
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)


if __name__ == '__main__':
    path = "/flume/original_scorer_input_data"
    save_path = "/tmp/marvin/dongfang"
    spark = SparkSession.builder.appName("dongfang").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_dongfang_data_dfp(sc=sc, path=path,
                          start="20180501", end="20180515",
                          save_path=save_path)
