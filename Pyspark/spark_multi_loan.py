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
    parser.add_argument('-r','--ratio',action='store',default=0.05,type=float,dest='ratio',help="maxent_id fraud ratio",nargs='+')
    parser.add_argument('-z','--zip_file',action='store',type=str,default=None,dest='zip_file',help="dependence zip file")
    args = parser.parse_args()
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k"
    save_path = "/tmp/qiancheng_sample_new"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()
    if args.zip_file is not None:
        abs_path = os.path.abspath(args.zip_file)
        print("add zip file {0}".format(abs_path))
        spark.sparkContext.addFile(abs_path)

    df_label = df_label.get_all_label_data(spark=spark, path=path)

    vld_label_1 = df_label.filter(col('label') == 1).drop_duplicates(subset=['maxent_id']).select('maxent_id')
    vld_label_1.persist(StorageLevel.DISK_ONLY)
    vld_label_1_num = vld_label_1.count()
    print "all valid unique maxent_id num is {0}".format(vld_label_1_num)

    maxent_id_label_0 = df_label.filter(col('label').isNull()).drop_duplicates(subset=['maxent_id']).select('maxent_id')
    maxent_id_label_0.persist(StorageLevel.DISK_ONLY)
    maxent_id_label_0_num = maxent_id_label_0.count()
    print "all label 0 maxent_id num is {0}".format(maxent_id_label_0_num)

    for ratio in args.ratio:
        ratio_num = int(round(1.0 / ratio)) -1
        print "user define label1 maxent_id ratio num is {0}".format(ratio_num)
        sample_num = vld_label_1_num * ratio_num
        print "sample label 0 num is {0}".format(sample_num)

        sample_label0_maxent_id = maxent_id_label_0.rdd.takeSample(False,sample_num)
        label0_maxent_id = spark.sparkContext.parallelize(sample_label0_maxent_id,100).toDF()
        label0_maxent_id.persist(StorageLevel.DISK_ONLY)
        label0_maxent_id_num = label0_maxent_id.count()
        print "all saumple unique label 0 maxent_id num is {0}".format(label0_maxent_id_num)

        maxent_id_label = label0_maxent_id.union(vld_label_1)
        maxent_id_label.persist(StorageLevel.DISK_ONLY)
        maxent_id_label_num= maxent_id_label.count()
        print "all unique label maxent_id num is {0}".format(maxent_id_label_num)

        df_final = df_label.join(maxent_id_label,'maxent_id','inner')
        df_final.persist(StorageLevel.DISK_ONLY)
        df_final_num =df_final.count()
        print "all event num is {0}".format(df_final_num)

        label0_num = df_final.filter(col('label').isNull()) \
            .drop_duplicates(subset=['maxent_id']).count()

        label1_ios_num = df_final.filter((col('os') == 'ios') & (col('label') == 1))\
            .drop_duplicates(subset=['maxent_id']).count()
        label1_android_num = df_final.filter((col('os') == 'android') & (col('label') == 1)) \
            .drop_duplicates(subset=['maxent_id']).count()
        label0_ios_num = df_final.filter((col('os') == 'ios') & (col('label').isNull())) \
            .drop_duplicates(subset=['maxent_id']).count()
        label0_android_num = df_final.filter((col('os') == 'android') & (col('label').isNull())) \
            .drop_duplicates(subset=['maxent_id']).count()
        try:
            label_ratio = 1.0 * vld_label_1_num / label0_num
        except Exception:
            label_ratio = 1.0
        try:
            label1_ratio = 1.0 * label1_ios_num / label1_android_num
        except Exception:
            label1_ratio = 1.0
        try:
            label0_ratio = 1.0 * label0_ios_num / label0_android_num
        except Exception:
            label0_ratio = 1.0
        print """
        label: label1 / label0: {0} / {1} = {2} \n 
        label 1: ios / android: {3} / {4} = {5}\n
        label 0: ios / android: {6} / {7} = {8}\n
        """.format(vld_label_1_num,label0_num,label_ratio,label1_ios_num,label1_android_num,\
                   label1_ratio, label0_ios_num,label0_android_num,label0_ratio)

        ratio_save_path = save_path + "_{0}".format(ratio)

        df_dev_final = event_to_dev.convert_event_dev(df=df_final)
        df_dev_final.write.mode("overwrite").option("header","true").csv(ratio_save_path)
