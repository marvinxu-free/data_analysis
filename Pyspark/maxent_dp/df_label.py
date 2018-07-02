# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file is used to get df_label data frame for event to maxent_id

"""
from pyspark.sql.functions import col

import multi_loan
from hive import get_label_maxentid
from join_op import *
from pyspark import StorageLevel
from read_data import getData


def get_all_android_v1(spark):
    df = spark.sql("""select maxent_id from odata.qiancheng_android_fraud""")
    return df


def get_all_ios_v1(spark):
    df = spark.sql("""select maxent_id from odata.qiancheng_ios_fraud""")
    return df


def get_sub_maxent_id(spark):
    sub = spark.sql("""select * from odata.qiancheng_all""")
    return sub


def get_uniq_col(df,col='maxent_id'):
    df_uniq = df.select(col).distinct()
    return df_uniq


def get_all_label_data(spark, path, tmp_df_path="/tmp/qiancheng_data_label"):
    """
    get qiancheng data with count first or from tmp file to speed up
    :param spark:
    :param path:
    :param tmp_df_path:
    :return:
    """
    try:
        df_label = spark.read.option("header","true").option("inferschema", "true").csv(tmp_df_path)
        df_label = df_label.filter(col('event_type') == 'ACT')
        df_label.persist(StorageLevel.DISK_ONLY)
        df_label_num =df_label.count()
        print "all event 1 num is {0}".format(df_label_num)
        return df_label
    except Exception:
        df = getData(path=path, spark=spark)
        df = df.filter(col('event_type') == 'ACT')
        df.persist(StorageLevel.DISK_ONLY)
        df_num = df.count()
        print "all sample data num is {0}".format(df_num)

        loan_df = multi_loan.get_multi_loan(spark=spark)
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

        label_id_df = get_label_maxentid(spark=spark)
        label_id_df.persist(StorageLevel.DISK_ONLY)
        label_mid_num =label_id_df.count()
        print "all valid maxent id v1.2 num is {0}".format(label_mid_num)

        df_label = join_state(df_count,label_id_df,'maxent_id')
        df_label.persist(StorageLevel.DISK_ONLY)
        df_label_num =df_label.count()
        print "all label 1 num is {0}".format(df_label_num)

        df_label.write.mode("overwrite").option("header", "true").csv(tmp_df_path)
        return df_label
