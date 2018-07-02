# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file is used to get uniq imei from beta during 0801-0820
which label is 1
"""

from pyspark.sql.functions import lit,col
from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
from pyspark import StorageLevel
import json
import re

import argparse
value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')

def readData(path,spark):
    """
    :param path: hdfs file path
    :return: data
    """
    data = spark.read.option("mergeSchema","true").json(path)
    data = data.filter("""day <= 20170820 and day >= 20170801""")
    data = data.toJSON()
    data = data.map(json.loads)
    data = data.repartition(100)
    data.persist(StorageLevel.DISK_ONLY)
    return data

def transData(record):
    a = {}
    a["maxent_id"] = record["content"]["maxent_id"]
    a["imei"] = record["content"]["did"].get('imei',None)
    return a

def getData(path,spark,tmp_path ='/tmp/qiancheng_imei_data'):
    try:
        df = spark.read.option("header","true").csv(tmp_path)
    except Exception:
        # Define the path of data before run getData
        data = readData(path,spark)
        data = data.filter(lambda x : x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k' \
            and x['content']['os'] == 'android') \
            .map(transData)
        data.persist(StorageLevel.DISK_ONLY)
        data_total = data.count()
        print "data of {0} if {1}".format('79rctxsz5nkuc9dd9qax3ewf5hay691k', data_total)
        df = spark.createDataFrame(data, samplingRatio=1)
        df.write.mode("overwrite").option("header", "true").csv(tmp_path)
    return df

def get_label_maxentid(spark):
    # mdf = spark.sparkContext.textFile(path)
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud_new""")
    return mdf

def join_sub(df,sub,col_name):
    df_new = df.join(sub,col_name,'inner')
    return  df_new

def join_state(df1,df2,col_name):
    df2 = df2.withColumn("label", lit(1))
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new

def get_imei(df,label_df):
    df_label = join_state(df,label_df,'maxent_id')
    df_imei= df_label.filter(col('label') == 1) \
        .drop_duplicates(subset=['imei']).select('imei')
    return df_imei

if __name__ == "__main__":
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k"
    save_path = "/tmp/qiancheng_label1_imei"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()
    df = getData(path=path,spark=spark)
    label1 = get_label_maxentid(spark=spark)
    df_imei = get_imei(df,label1)
    df_imei.coalesce(1).write.mode("overwrite").option("header", "false").csv(save_path)

