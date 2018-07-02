# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from maxent_dp import df_label
from maxent_dp import event_to_dev
from pyspark import StorageLevel

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-z','--zip_file',action='store',type=str,default=None,dest='zip_file',help="dependence zip file")
    args = parser.parse_args()
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k"
    save_path = "/tmp/qiancheng_dev"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()
    if args.zip_file is not None:
        abs_path = os.path.abspath(args.zip_file)
        print("add zip file {0}".format(abs_path))
        spark.sparkContext.addFile(abs_path)

    df_event = df_label.get_all_label_data(spark=spark, path=path)
    df_event.persist(StorageLevel.DISK_ONLY)
    df_event_num =df_event.count()
    print "all event num is {0}".format(df_event_num)

    maxent_id_num = df_event.drop_duplicates(subset=['maxent_id']).count()
    print("all maxent_id num is {0}".format(maxent_id_num))

    label0_num = df_event.filter(col('label').isNull()) \
        .drop_duplicates(subset=['maxent_id']).count()

    label1_ios_num = df_event.filter((col('os') == 'ios') & (col('label') == 1))\
        .drop_duplicates(subset=['maxent_id']).count()
    label1_android_num = df_event.filter((col('os') == 'android') & (col('label') == 1)) \
        .drop_duplicates(subset=['maxent_id']).count()
    label0_ios_num = df_event.filter((col('os') == 'ios') & (col('label').isNull())) \
        .drop_duplicates(subset=['maxent_id']).count()
    label0_android_num = df_event.filter((col('os') == 'android') & (col('label').isNull())) \
        .drop_duplicates(subset=['maxent_id']).count()
    try:
        label1_ratio = 1.0 * label1_ios_num / label1_android_num
    except Exception:
        label1_ratio = 1.0
    try:
        label0_ratio = 1.0 * label0_ios_num / label0_android_num
    except Exception:
        label0_ratio = 1.0

    df_dev = event_to_dev.convert_event_dev(df=df_event)
    df_dev.write.mode("overwrite").option("header","true").csv(save_path)


