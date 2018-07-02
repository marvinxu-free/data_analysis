# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pyspark import StorageLevel
from pyspark.sql.functions import col
import json
import re
from udf_functions import udf_all
from time_utils import get_day_range

value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')


def readData(path, spark, filter_str="""day <= 20170820 and day >= 20170801"""):
    """

    :param path:
    :param spark:
    :param filter_str:
    :return:
    """
    print("begin to read data with filter condition: {0}".format(filter_str))
    data = spark.read.option("mergeSchema","true").json(path)
    data = data.filter(filter_str)
    data = data.toJSON()
    data = data.map(json.loads)
    data = data.repartition(400)
    data.persist(StorageLevel.DISK_ONLY)
    return data


def transData(record):
    a = {}
    metircs = record["metrics"][0]["metrics"]
    b = {}
    b['scenario'] = record["metrics"][0]["scenario"]
    for (x, y) in metircs.items():
        value_flg = value_match.match(x)
        anomaly_flg = anomaly_match.match(x)
        if value_flg or anomaly_flg:
            key = str(x).replace(".","_")
            b[key] = y
    derived = {}
    if "derived" in record:
        if "ipGeo" in record["derived"]:
            derived["ipGeo"] = record["derived"]['ipGeo']
        if "ipSeg24" in record["derived"]:
            derived["ipSeg24"] = record["derived"]['ipSeg24']
    a.update(b)
    a.update(derived)
    a["timestamp"] = float(record["content"]["timestamp"])
    a["event_id"] = record["content"]["event_id"]
    a["maxent_id"] = record["content"]["maxent_id"]
    a["event_type"] = record["event_type"]
    a["os"] = record["content"]["os"]
    return a


def getData(path,spark,tmp_path ='/tmp/qiancheng_parsed_data'):
    try:
        df = spark.read.option("header","true").option("inferschema", "true").csv(tmp_path)
    except Exception:
        data = readData(path,spark)
        data = data.filter(lambda x:x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k') \
            .map(transData)
        data.persist(StorageLevel.DISK_ONLY)
        data_total = data.count()
        print "data of {0} if {1}".format('79rctxsz5nkuc9dd9qax3ewf5hay691k', data_total)
        df_tmp_0 = spark.createDataFrame(data, samplingRatio=1)
        df_tmp = udf_all(df=df_tmp_0)
        cols = df_tmp.columns
        print("all cols is {0}".format(cols))
        anomaly_pattern = re.compile(r'.*anomaly$')
        anomaly_cols = filter(lambda x:anomaly_pattern.match(x),cols)
        value_pattern = re.compile(r'.*value$')
        value_cols = filter(lambda x:value_pattern.match(x),cols)
        count_pattern = re.compile(r'.*counts$')
        count_cols = filter(lambda x:count_pattern.match(x),cols)
        print("count cols is {0}".format(count_cols))
        loan_pattern = re.compile(r'.*loan$')
        loan_cols = filter(lambda x:loan_pattern.match(x),cols)
        cast_cols = anomaly_cols + value_cols + count_cols + loan_cols
        casted_cols = map(lambda c:col(c).cast("float").alias(c), cast_cols)
        no_cast_cols = filter(lambda x:x not in cast_cols,cols)
        no_cast_cols = map(lambda x:col(x), no_cast_cols)
        new_cols = casted_cols + no_cast_cols
        df = df_tmp.select(*new_cols)
        df.persist(StorageLevel.DISK_ONLY)
        df_num = df.count()
        print "caste df num is {0}".format(df_num)
        df.printSchema()
        df.write.mode("overwrite").option("header", "true").csv(tmp_path)
    return df


def read_data_by_days(sc, path, start, end):
    """
    this function used read data from some day in daylist
    sc should be recursive configured
    :param sc:
    :param path:
    :param start:
    :param end:
    :return:
    """
    day_list = get_day_range(start, end)
    print "get data from {0} to {1}".format(day_list[0], day_list[-1])
    day_paths = map(lambda x: "{0}/day={1}".format(path, x), day_list)
    day_paths_str = ",".join(day_paths)
    rdd = sc.textFile(day_paths_str)
    return rdd

