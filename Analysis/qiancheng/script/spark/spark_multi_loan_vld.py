# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pyspark.sql.functions import lit,col
from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
from pyspark import StorageLevel
import json
import argparse
import re

value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')

def readData(path,spark):
    """
    :param path: hdfs file path
    :return: data
    """
    data = spark.read.option("mergeSchema","true").json(path)
    # data = data.filter("""day <= 20170820 and day >= 20170801""")
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

def getData(path,spark,tmp_path ='/tmp/qiancheng_parsed_data'):
    try:
        df = spark.read.option("header","true").csv(tmp_path)
    except Exception:
        # Define the path of data before run getData
        data = readData(path,spark)
        data = data.filter(lambda x:x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k') \
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

def get_uniq_col(df,col='maxent_id'):
    df_uniq = df.select(col).distinct()
    return df_uniq

def get_vld_data(spark,df,df_vld,os='ios',ratio_num=99):
    vld_all = df.join(df_vld, 'maxent_id', 'inner')
    vld_all.persist(StorageLevel.DISK_ONLY)
    vld_all_num = vld_all.count()
    print "all {0} vld num is {1}".format(os,vld_all_num)

    uniq_all_maxent_id = get_uniq_col(df=vld_all)
    uniq_all_maxent_id.persist(StorageLevel.DISK_ONLY)
    uniq_all_maxent_id_num = uniq_all_maxent_id.count()
    uniq_all_maxent_id_cols = uniq_all_maxent_id.columns
    print "all {0} unique maxent_id num is {1}:{2}".format(os,uniq_all_maxent_id_num,uniq_all_maxent_id_cols)

    vld_all_label_1= vld_all.filter((col('os') == os) & (col('label') == 1)) \
        .drop_duplicates(subset=['maxent_id']).select('maxent_id')
    vld_all_label_1.persist(StorageLevel.DISK_ONLY)
    vld_all_label_1_num = vld_all_label_1.count()
    vld_all_label_1_cols = vld_all_label_1.columns
    print "all {0} unique label 1 maxent_id num is {1},cols is {2}".format(os,vld_all_label_1_num,vld_all_label_1_cols)

    vld_all_label_1_list = vld_all_label_1.select("maxent_id").rdd.flatMap(lambda x: x).collect()

    maxent_id_label_0 = uniq_all_maxent_id.filter(~uniq_all_maxent_id.maxent_id.isin(vld_all_label_1_list))
    # maxent_id_label_0 = uniq_all_maxent_id.subtract(vld_all_label_1)
    maxent_id_label_0.persist(StorageLevel.DISK_ONLY)
    maxent_id_label_0_num = maxent_id_label_0.count()
    print "all {0} unique label 0 maxent_id num is {1}".format(os,maxent_id_label_0_num)

    sample_num = ratio_num * vld_all_label_1_num
    print "sample {0} label 0 num is {1}".format(os,sample_num)
    sample_label0_maxent_id = maxent_id_label_0.rdd.takeSample(False,sample_num)
    label0_maxent_id = spark.sparkContext.parallelize(sample_label0_maxent_id,100).toDF()
    label0_maxent_id.persist(StorageLevel.DISK_ONLY)
    label0_maxent_id_num = label0_maxent_id.count()
    print "all saumple unique label 0 maxent_id num is {0}".format(label0_maxent_id_num)

    maxent_id_label = label0_maxent_id.union(vld_all_label_1)
    maxent_id_label.persist(StorageLevel.DISK_ONLY)
    maxent_id_label_num= maxent_id_label.count()
    print "all unique label maxent_id num is {0}".format(maxent_id_label_num)

    df_final = vld_all.join(maxent_id_label,'maxent_id','inner')
    df_final.persist(StorageLevel.DISK_ONLY)
    df_final_num = df_final.count()
    print "all event num is {0}".format(df_final_num)
    label1_num = df_final.filter((col('os') == os) & (col('label') == 1)) \
        .drop_duplicates(subset=['maxent_id']).count()
    label0_num = df_final.filter((col('os') == os) & (col('label').isNull())) \
        .drop_duplicates(subset=['maxent_id']).count()
    try:
        label_ratio = 1.0 * label1_num / label0_num
    except Exception:
        label_ratio = 1.0
    print """{0} state:\n\tlabel 1: num = {1}
    \n\tlabel 0: num = {2}\n\t ratio = {3}\n
    """.format(os,label1_num,label0_num,label_ratio)
    return df_final

def get_sub_maxent_id(spark):
    sub = spark.sql("""select * from odata.qiancheng_all""")
    return sub

def get_all_data_pre(spark,path,tmp_path="/tmp/qiancheng_data_label_vld"):
    """
    get qiancheng data with count first or from tmp file to speed up
    :param spark:
    :param path:
    :param tmp_path:
    :return:
    """
    try:
        df_label = spark.read.option("header","true").csv(tmp_path)
        df_label.persist(StorageLevel.DISK_ONLY)
        df_label_num = df_label.count()
        print "all loan count label num is {0}".format(df_label_num)
    except Exception:
        df = getData(path=path,spark=spark)
        df.persist(StorageLevel.DISK_ONLY)
        df_num = df.count()
        print "all sample data num is {0}".format(df_num)

        loan_df = get_multi_loan(spark=spark)
        loan_df.persist(StorageLevel.DISK_ONLY)
        loan_df_num =loan_df.count()
        print "all multi loan num is {0}".format(loan_df_num)

        df_loan= join_loan(df,loan_df)
        df_loan.persist(StorageLevel.DISK_ONLY)
        df_loan_num = df_loan.count()
        print "all loan data num is {0}".format(df_loan_num)
        df_count_pre = get_sub_maxent_id(spark=spark)
        df_count = join_loan(df_loan,df_count_pre)
        df_count.persist(StorageLevel.DISK_ONLY)
        df_count_num = df_count.count()
        print "all loan count num is {0}".format(df_count_num)

        label_id_df_v1 = get_label_v1_maxentid(spark=spark)
        label_id_df_v1.persist(StorageLevel.DISK_ONLY)
        label_mid_num_v1 =label_id_df_v1.count()
        print "all valid maxent id v1 num is {0}".format(label_mid_num_v1)

        df_label = join_state(df_count,label_id_df_v1,'maxent_id')
        df_label.persist(StorageLevel.DISK_ONLY)
        df_label_num =df_label.count()
        print "all label 1 num is {0}".format(df_label_num)
        df_label.write.mode("overwrite").option("header", "true").csv(tmp_path)
    return df_label

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r','--ratio',action='store',default=0.05,type=float,dest='ratio',help="maxent_id fraud ratio",nargs='+')
    args = parser.parse_args()
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k"
    save_path = "/tmp/qiancheng_sample_new"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()

    df_label = get_all_data_pre(spark=spark,path=path)

    android_vld = get_vld_android(spark=spark)
    android_vld.persist(StorageLevel.DISK_ONLY)
    android_vld_num =android_vld.count()
    print "all android vld maxent id num is {0}".format(android_vld_num)

    ios_vld = get_vld_ios(spark=spark)
    ios_vld.persist(StorageLevel.DISK_ONLY)
    ios_vld_num =ios_vld.count()
    print "all ios vld maxent id num is {0}".format(ios_vld_num)

    for ratio in args.ratio:
        ratio_num = int(round(1.0 / ratio)) -1
        # sample_num = label_mid_num_v1 * ratio_num
        test_path = save_path + "_{0}".format(ratio)
        print "\n\n----------------"
        print "user define label1 maxent_id ratio num is {0}:{1}:{2}".format(ratio_num,ratio,test_path)
        train_data = spark.read.option("header","true").csv(test_path)
        sub_zero = train_data.filter(col('label').isNull())
        sub_zero.persist(StorageLevel.DISK_ONLY)
        train_zero_num = sub_zero.count()
        print "train data zero num is {0}".format(train_zero_num)
        df_label_sub = df_label.subtract(sub_zero)
        df_label_sub.persist(StorageLevel.DISK_ONLY)
        df_label_sub_num =df_label_sub.count()
        print "df label sub test data zero num is {0}".format(df_label_sub_num)
        vld_ios_all_label_final = get_vld_data(spark=spark,df=df_label_sub,df_vld=ios_vld,\
                                           ratio_num=ratio_num)
        vld_ios_all_label_final.persist(StorageLevel.DISK_ONLY)
        vld_ios_all_label_final_num = vld_ios_all_label_final.count()
        print "vld ios sample num is {0}".format(vld_ios_all_label_final_num)
        vld_ios_all_label_final.write.mode("overwrite").option("header","true").csv('/tmp/qiancheng_ios_vld_{0}'.format(ratio))

        vld_android_all_label_final = get_vld_data(spark=spark,df=df_label_sub,df_vld=android_vld,\
                                               os='android',ratio_num=ratio_num)
        vld_android_all_label_final.persist(StorageLevel.DISK_ONLY)
        vld_android_all_label_final_num = vld_android_all_label_final.count()
        print "vld android sample num is {0}".format(vld_android_all_label_final_num)
        vld_android_all_label_final.write.mode("overwrite").option("header","true").csv('/tmp/qiancheng_android_vld_{0}'.format(ratio))
