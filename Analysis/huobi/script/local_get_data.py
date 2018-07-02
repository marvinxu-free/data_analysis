# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/8/24
# Company : Maxent
# Email: chao.xu@maxent-inc.com

import numpy as np
import pandas as pd
import os
import json

os.environ["SPARK_HOME"] = "/Users/chaoxu/software/spark-2.1.1-bin-hadoop2.7"
from operator import add
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from Pic.check_model import scored


def transAnomaly(record):
    a = record.copy()
    score = 0.0
    num = 0
    for i in record:
        if "anomaly" in i:
            num += 1
            _log = np.log(record[i]) if record[i] is not None else 0.0
            score += _log
            a["%s.log" % i] = _log
    a["score"] = scored(score, num)
    return a


def getData(path, spark):
    # Define the path of data before use getDataFrame.
    data = spark.read.parquet(path)
    data = data.rdd.map(lambda x: x.asDict())
    data = data.map(lambda x: transAnomaly(x))
    data = data.filter(lambda x: x["score"] >= 90 and x["ipGeo.1d.value"] == 7030)
    return data


def mergeProxy(a, b):
    if a == "true" or b == "true":
        return "true"
    else:
        return "false"


if __name__ == "__main__":
    path = "/Users/chaoxu/code/local-spark/Data/huobi"
    save_path = "/Users/chaoxu/code/local-spark/Data"
    spark = SparkSession.builder.appName("huobi").getOrCreate()
    data = getData(path, spark)
    data.saveAsTextFile(save_path + "ipGeo")
