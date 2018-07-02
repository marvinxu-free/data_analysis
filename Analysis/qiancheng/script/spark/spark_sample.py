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

a = """
select (select count(maxent_id) from odata.qiancheng_fraud") 
- (SELECT COUNT(maxent_id) from odata.qiancheng_all 
where imei_counts > 2 
or mac_counts > 1 
or mcid_counts > 5 
or aid_counts > 1 
or idfv_counts >3 
or idfa_counts > 3) as sub_num;
"""

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
    a["os"] = record["content"]["os"]
    return a

def getData(path,sub,col_name,spark):
    # Define the path of data before run getData
    data = readData(path,spark)
    data = data.filter(lambda x:x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k') \
        .map(transData)
    # data.persist(StorageLevel.DISK_ONLY)
    # data_total = data.count()
    # print "data of {0} if {1}".format('79rctxsz5nkuc9dd9qax3ewf5hay691k', data_total)
    df1 = spark.createDataFrame(data, samplingRatio=1)
    df = df1.join(sub,col_name,'inner')
    # df_rdd = df.rdd.map(json.loads)
    # df_rdd.persist(StorageLevel.DISK_ONLY)
    # df_rdd_num = df_rdd.count()
    # print "data in valid sub of {0} is {1}".format('79rctxsz5nkuc9dd9qax3ewf5hay691k', df_rdd_num)
    # data_android_sample = df_rdd.filter(lambda x:x['os'] == "android").takeSample(False,10000)
    # data_android = spark.sparkContext.parallelize(data_android_sample)
    # data_ios_sample = df_rdd.filter(lambda x:x['os'] == "ios").takeSample(False,10000)
    # data_ios = spark.sparkContext.parallelize(data_ios_sample)
    # data_ = data_android.union(data_ios)
    # df = spark.createDataFrame(data_, samplingRatio=1)
    return df

def get_label_maxentid(path,spark):
    # mdf = spark.sparkContext.textFile(path)
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud""")
    return mdf

def get_sub_maxent_id(spark,os='ios'):
    # if os == 'ios':
    #     sub = spark.sql("""select maxent_id where idfv_counts >3 or idfa_counts > 3""")
    # else :
    #     sub = spark.sql("""select maxent_id where imei_counts > 2 or mac_counts > 1 or mcid_counts > 5 or aid_counts > 1""")
    sub = spark.sql("""select * from odata.qiancheng_all
    where imei_counts > 2 
    or mac_counts > 1 
    or mcid_counts > 5 
    or aid_counts > 1
    or idfv_counts >3
    or idfa_counts > 3
    """)
    return sub

def join_sub(df,sub,col_name):
    df_new = df.join(sub,col_name,'inner')
    return  df_new

def join_state(df1,df2,col_name):
    df2 = df2.withColumn("label", lit(1))
    # vld_not_appear = df2.select(col_name).subtract(df1.select(col_name))
    # vld_not_appear.persist(StorageLevel.DISK_ONLY)
    # vld_not_appear_num = vld_not_appear.count()
    # vld_not_appear.coalesce(1).write.mode("overwrite").option("header","false").csv("/tmp/qiancheng_not_appear_id")
    # print "valid maxent id not appear in beta num is {0} ".format(vld_not_appear_num)
    # df_new = df1.join(df2, col_name, "left_outer").na.fill({"label":0})
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new

if __name__ == "__main__":
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k_bak1/*"
    maxent_id_path = "/tmp/qiancheng_fraud_list/qiancheng_fraud_list.txt"
    save_path = "/tmp/qiancheng_sample"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()

    sub_df = get_sub_maxent_id(spark)
    sub_df.persist(StorageLevel.DISK_ONLY)
    sub_df_num =sub_df.count()
    print "all sub rule maxent id num is {0}".format(sub_df_num)

    label_id_df = get_label_maxentid(path=maxent_id_path,spark=spark)
    label_id_df.persist(StorageLevel.DISK_ONLY)
    label_mid_num =label_id_df.count()
    print "all valid maxent id num is {0}".format(label_mid_num)

    df_label = join_state(df1=sub_df,df2=label_id_df,col_name="maxent_id")
    df_label.persist(StorageLevel.DISK_ONLY)
    df_label_num =df_label.count()
    print "all label maxent id num is {0}".format(df_label_num)

    df = getData(path=path,sub=df_label,col_name='maxent_id',spark=spark)
    df.persist(StorageLevel.DISK_ONLY)
    df_num = df.count()
    print "all sample data num is {0}".format(df_num)
    df.coalesce(1).write.mode("overwrite").option("header","true").csv(save_path)

    # df_new.coalesce(1).write.mode("overwrite").option("header","true").csv(save_path)
