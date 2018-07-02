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
from py4j.protocol import Py4JJavaError

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
    data = spark.read.option("mergeSchema","true").json(path)
    data = data.filter("""day <= 20170820 and day >= 20170801""")
    data = data.toJSON()
    data = data.map(json.loads)
    data = data.repartition(100)
    data.persist(StorageLevel.DISK_ONLY)
    return data

def get_multi_loan(spark,path = '/tmp/multi-loan'):
    try:
        df = spark.read.json(path)
    except Exception as e:
        df = spark.sql("""
    select a.maxent_id, case when b.mac is not null then 1 else 0 end as mac_loan
,case when c.imei is not null then 1 else 0 end as imei_loan
,case when d.aid is not null then 1 else 0 end as aid_loan
,case when e.idfa is not null then 1 else 0 end as idfa_loan
from
(select maxent_id, mac, imei, aid, idfa from fdata.id_system_maxentid
where  day >= 20170715 and day <= 20170815 and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k') a
left join
(select distinct a.mac from
(select mac, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and mac not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by mac) a join (
select mac from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.mac = b.mac
where a.num > 1) b on a.mac = b.mac
left join
(select distinct a.imei from
(select imei, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and imei not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by imei) a join (
select imei from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.imei = b.imei
where a.num > 1) c on a.imei = c.imei
left join
(select distinct a.aid from
(select aid, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and aid not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by aid) a join (
select aid from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.aid = b.aid
where a.num > 1) d on a.aid = d.aid
LEFT JOIN
(select distinct a.idfa from
(select idfa, count(distinct tid) as num
from fdata.id_system_maxentid
where tid in ('79rctxsz5nkuc9dd9qax3ewf5hay691k',
'f4fe3f2bfd403d75905d1a5385aa66fc',
'c0155306e4aa3138a83143ff2b5685bd',
'jwm90s9mnzr033rsew6yz0jxwqal7xom',
'8enu94eqhvol15pe71als1ojbo0fe147')
and day >= 20170715 and day <= 20170815
and idfa not in ('02:00:00:00:00:00',
'00:00:00:00:00:00',
'unknown',
'ff:ff:ff:ff:ff:ff',
'00-00-00-00-00-00-00-E0',
':::::')
group by idfa) a join (
select idfa from fdata.id_system_maxentid
where day >= 20170715 and day <= 20170815
and tid = '79rctxsz5nkuc9dd9qax3ewf5hay691k'
) b on a.idfa = b.idfa
where a.num > 1) e on a.idfa = e.idfa
""")
        df.write.mode('overwrite').json(path)
    return df


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
    # a["message_timestamp"] = record["content"]["message"]["__timestamp"]
    a["timestamp"] = record["content"]["timestamp"]
    a["event_id"] = record["content"]["event_id"]
    a["maxent_id"] = record["content"]["maxent_id"]
    a["event_type"] = record["event_type"]
    a["os"] = record["content"]["os"]
    return a

def getData(path,spark):
    # Define the path of data before run getData
    data = readData(path,spark)
    data = data.filter(lambda x:x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k') \
        .map(transData)
    data.persist(StorageLevel.DISK_ONLY)
    data_total = data.count()
    print "data of {0} if {1}".format('79rctxsz5nkuc9dd9qax3ewf5hay691k', data_total)
    df = spark.createDataFrame(data, samplingRatio=1)
    return df

def get_label_maxentid(spark):
    # mdf = spark.sparkContext.textFile(path)
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud_new""")
    return mdf

def get_label_v1_maxentid(spark):
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud""")
    return mdf

def get_vld_ios(spark,path='/tmp/qiancheng_ios_vld'):
    ios_vld = spark.sql("""select maxent_id from odata.qiancheng_ios_fraud""")

    return ios_vld

def get_vld_android(spark,path='/tmp/qiancheng_android_vld'):
    android_vld = spark.sql("""select maxent_id from odata.qiancheng_android_fraud""")
    return android_vld

def join_sub(df,sub,col_name):
    df_new = df.join(sub,col_name,'inner')
    return  df_new

def join_state(df1,df2,col_name):
    df2 = df2.withColumn("label", lit(1))
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new

def join_loan(df1,df2,col_name='maxent_id'):
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new
def get_all_android_v1(spark):
    df = spark.sql("""select maxent_id from odata.qiancheng_android_fraud""")
    return df
def get_all_ios_v1(spark):
    df = spark.sql("""select maxent_id from odata.qiancheng_ios_fraud""")
    return df

def get_sub_maxent_id(spark):
    sub = spark.sql("""select * from odata.qiancheng_all""")
    return sub

if __name__ == "__main__":
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k"
    save_path = "/tmp/qiancheng_sample_anomaly"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()

    loan_df = get_multi_loan(spark=spark)
    loan_df.persist(StorageLevel.DISK_ONLY)
    loan_df_num =loan_df.count()
    print "all multi loan num is {0}".format(loan_df_num)

    label_id_df = get_label_maxentid(spark=spark)
    label_id_df.persist(StorageLevel.DISK_ONLY)
    label_mid_num =label_id_df.count()
    print "all valid maxent id v1.2 num is {0}".format(label_mid_num)

    df = getData(path=path,spark=spark)
    df.persist(StorageLevel.DISK_ONLY)
    df_num = df.count()
    print "all sample data num is {0}".format(df_num)

    df_loan= join_loan(df,loan_df)
    df_loan.persist(StorageLevel.DISK_ONLY)
    df_loan_num = df_loan.count()
    print "all loan data num is {0}".format(df_loan_num)

    df_count_pre = get_sub_maxent_id(spark=spark)
    df_count = join_loan(df_loan,df_count_pre)
    df_count.persist(StorageLevel.DISK_ONLY)
    df_count_num = df_count.count()
    print "all loan count num is {0}".format(df_count_num)

    df_label = df_count.join(label_id_df,'maxent_id','inner')
    df_label.persist(StorageLevel.DISK_ONLY)
    df_label_num =df_label.count()
    print "all label 1 num is {0}".format(df_label_num)

    df_label.coalesce(1).write.mode("overwrite").json(save_path)
