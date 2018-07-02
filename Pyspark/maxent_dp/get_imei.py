# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/13
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to get imeis and cr
"""
from read_data import readData
from df_label import join_state
from pyspark import StorageLevel
from pyspark.sql.functions import col, lit
import re
from join_op import join_inner, join_left_outer, join_leftanti
from hive import get_label_v1_maxentid, get_label_maxentid, get_qiancheng_android_pred, get_qiancheng_ios_pred
from udf_functions import encrypt_imeis

value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')


def extraData(record):
    a = {}
    a["event_id"] = record["content"]["event_id"]
    a["maxent_id"] = record["content"]["maxent_id"]
    a["event_type"] = record["event_type"]
    a["imei"] = record["content"]["did"].get('imei', None)
    a["cr"] = record["content"].get('cr', None)
    return a


def getData(path, spark, tmp_path='/tmp/qiancheng_imei_data'):
    try:
        df = spark.read.option("header", "true").csv(tmp_path)
    except Exception:
        data = readData(path, spark)
        data = data.filter(lambda x: x['content']['tid'] == '79rctxsz5nkuc9dd9qax3ewf5hay691k' \
                                     and x['content']['os'] == 'android') \
            .map(extraData)
        data.persist(StorageLevel.DISK_ONLY)
        data_total = data.count()
        print "data of {0} if {1}".format('79rctxsz5nkuc9dd9qax3ewf5hay691k', data_total)
        df = spark.createDataFrame(data, samplingRatio=1)
        df.write.mode("overwrite").option("header", "true").csv(tmp_path)
    return df


def get_imei(path, spark, save_path="/tmp/qiancheng_cr_imei"):
    df = getData(path=path, spark=spark)
    label_df = get_label_maxentid(spark=spark)
    df_label = join_state(df, label_df, 'maxent_id')
    df_imei1 = df_label.filter(col('label') == 1).drop_duplicates(subset=['imei']).select(['imei', "cr"])
    df_imei1.persist(StorageLevel.DISK_ONLY)
    df_imei1_num = df_imei1.count()
    print "imei1 num is {0}".format(df_imei1_num)
    df_imei0 = df_label.filter(col('label').isNull()).drop_duplicates(subset=['imei']).select(['imei', "cr"])
    sample_num = df_imei1_num * 10

    sample_label0_imei = df_imei0.rdd.takeSample(False, sample_num)
    label0_imei = spark.sparkContext.parallelize(sample_label0_imei, 100).toDF()
    label0_imei.persist(StorageLevel.DISK_ONLY)
    label0_imei_num = label0_imei.count()
    print "all saumple unique label 0 maxent_id num is {0}".format(label0_imei_num)

    imeis = label0_imei.select(['imei', 'cr']).union(df_imei1.select(['imei', 'cr']))
    imeis.persist(StorageLevel.DISK_ONLY)
    imeis_num = imeis.count()
    print "all imeis num is {0}".format(imeis_num)
    imeis.coalesce(1).write.mode("overwrite").option("header", "false").csv(save_path)


def get_imei_by_maxent_ids(df, mdf, num, spark, fraud_type, mode="in"):
    """
    get data based on maxent_ids
    :param df: total data
    :param mdf: maxent_id data frame
    :param num: need num
    :param spark:
    :param fraud_type: fraud type: from where or normal
    :param mode: join mode
    :return: df with imei and cr only
    """
    if mode == 'in':
        df_new_tmp = join_inner(df=df, sub=mdf, col_name='maxent_id')
    elif mode == 'notin':
        df_new_tmp = join_leftanti(df1=df, df2=mdf, col_name='maxent_id')
    else:
        exit(1)
    df_new = df_new_tmp.drop_duplicates(subset=['imei']).select(['imei', 'cr'])
    samples = df_new.rdd.takeSample(False, num=num)
    samples_df = spark.sparkContext.parallelize(samples, 100).toDF()
    samples_df = samples_df.withColumn('fraud_type', lit(fraud_type))
    samples_df.persist(StorageLevel.DISK_ONLY)
    samples_df_num = samples_df.count()
    print "mode {0} get IMEIs from {1} num is {2}".format(mode, fraud_type, samples_df_num)
    return samples_df, samples_df_num


def get_imei_for_qiaoda(spark, fname, save_path="/tmp/qiaoda_cr_imei"):
    """
    this function used to get IMEIs for qiaoda
    :param spark:
    :param fname:
    :return:
    """
    print("get qiancheng fraud maxent_ids")
    qiancheng_fraud_v1 = get_label_v1_maxentid(spark=spark)
    qiancheng_fraud_v2 = get_label_maxentid(spark=spark)
    print("get maxent predict fraud maxent_ids")
    maxent_ios_fraud_pred = get_qiancheng_ios_pred(spark=spark)
    maxent_android_fraud_pred = get_qiancheng_android_pred(spark=spark)

    qiancheng_fraud = qiancheng_fraud_v1.union(qiancheng_fraud_v2)
    qiancheng_fraud.persist(StorageLevel.DISK_ONLY)
    qiancheng_fraud_nums = qiancheng_fraud.count()
    print("all qiancheng fraud num is {0}".format(qiancheng_fraud_nums))
    maxent_fraud = maxent_android_fraud_pred.union(maxent_ios_fraud_pred)
    maxent_fraud.persist(StorageLevel.DISK_ONLY)
    maxent_fraud_nums = maxent_fraud.count()
    print("all maxent predict fraud num is {0}".format(maxent_fraud_nums))

    maxent_only_fraud = maxent_fraud.subtract(qiancheng_fraud)
    maxent_only_fraud.persist(StorageLevel.DISK_ONLY)
    maxent_only_fraud_nums = maxent_only_fraud.count()
    print("only maxent predict fraud num is {0}".format(maxent_only_fraud_nums))

    all_fraud_maxent_ids = qiancheng_fraud.union(maxent_only_fraud)
    all_fraud_maxent_ids.persist(StorageLevel.DISK_ONLY)
    all_fraud_maxent_ids_nums = all_fraud_maxent_ids.count()
    print("all fraud maxent_ids num is {0}".format(all_fraud_maxent_ids_nums))

    all_data = getData(path=fname, spark=spark)

    qiancheng_imeis, qiancheng_imeis_num = get_imei_by_maxent_ids(df=all_data, mdf=qiancheng_fraud, num=10000,
                                                                  spark=spark, fraud_type='qiancheng')
    maxent_imeis, maxent_imeis_num = get_imei_by_maxent_ids(df=all_data, mdf=maxent_only_fraud,
                                                            num=10000 - qiancheng_imeis_num, spark=spark,
                                                            fraud_type='maxent')
    nor_data, nor_data_num = get_imei_by_maxent_ids(df=all_data, mdf=all_fraud_maxent_ids, num=10000, spark=spark,
                                                    mode="notin", fraud_type="normal")
    cols = ['imei', 'cr', "fraud_type"]
    df_tmp = nor_data.select(*cols).union(qiancheng_imeis.select(*cols)).union(maxent_imeis.select(*cols))
    df = encrypt_imeis(df=df_tmp)
    df.persist(StorageLevel.DISK_ONLY)
    df_num = df.count()
    print("get IMEIs num is {0}".format(df_num))
    df.coalesce(1).write.mode("overwrite").option("header", "false").csv(save_path)
    print("IMEIs saved to {0}".format(save_path))
