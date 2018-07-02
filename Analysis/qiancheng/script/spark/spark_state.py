# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
from pyspark import StorageLevel
import json
import re

value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')

def readData(path,spark):
    """
    :param path: hdfs file path
    :return: data
    """
    data = spark.sparkContext.textFile(path)
    data = data.map(json.loads)
    data = data.repartition(100)
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
            b[x] = y
    derived = {}
    if  "derived" in record:
        if "ipGeo" in record["derived"]:
            derived["ipGeo"] = record["derived"]['ipGeo']
        if "ipSeg24" in record["derived"]:
            derived["ipSeg24"] = record["derived"]['ipSeg24']
    a.update(b)
    a.update(derived)
    a["message_timestamp"] = record["content"]["message"]["__timestamp"]
    a["timestamp"] = record["content"]["timestamp"]
    a["event_id"] = record["content"]["event_id"]
    a["maxent_id"] = record["content"]["maxent_id"]
    a["event_type"] = record["event_type"]
    return a

def getData(path,spark):
    # Define the path of data before run getData
    data = readData(path,spark)
    data = data.filter(lambda x:x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k') \
        .map(transData)
    df = spark.createDataFrame(data, samplingRatio=1)
    return df

def get_valid_maxentid(path,spark):
    # mdf = spark.sparkContext.textFile(path)
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud""")
    return mdf

def join_state(df1,df2,col_name):
    df2 = df2.withColumn("label", lit(1))
    vld_not_appear = df2.select(col_name).subtract(df1.select(col_name))
    vld_not_appear.persist(StorageLevel.DISK_ONLY)
    vld_not_appear_num = vld_not_appear.count()
    # print "maxent id in beta not appear in valid maxent file num is %s ".format(vld_not_appear_num)
    vld_not_appear.coalesce(1).write.mode("overwrite").option("header","false").csv("/tmp/qiancheng_not_appear_id")
    print "valid maxent id not appear in beta num is {0} ".format(vld_not_appear_num)
    # df_new = df1.join(df2, col_name, "left_outer").na.fill({"label":0})
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new

if __name__ == "__main__":
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k_bak1/*"
    maxent_id_path = "/tmp/qiancheng_fraud_list/qiancheng_fraud_list.txt"
    save_path = "/tmp/qiancheng"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()
    vld_id_df = get_valid_maxentid(path=maxent_id_path,spark=spark)
    vld_id_df.persist(StorageLevel.DISK_ONLY)
    vld_mid_num =vld_id_df.count()
    print "all valid maxent id num is {0}".format(vld_mid_num)
    df = getData(path=path,spark=spark)
    df.persist(StorageLevel.DISK_ONLY)
    df_num = df.count()
    print "all beta data num is {0}".format(df_num)
    df_new = join_state(df1=df,df2=vld_id_df,col_name="maxent_id")
    df_new.coalesce(1).wite.mode("overwrite").option("header","true").csv("/tmp/qiancheng")
