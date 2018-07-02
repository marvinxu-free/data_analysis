# -*- coding: utf-8 -*-
# Project: maxent-ml
# Author: chaoxu create this file
# Time: 2017/9/4
# Company : Maxent
# Email: chao.xu@maxent-inc.com
# this file is used to run spark local
from pyspark.sql import SparkSession
import argparse


def read_file(spark,path,out_file):
    df = spark.read.option("header","true").csv(path)
    df.coalesce(1).write.mode("overwrite").option("header","true").csv(out_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r','--ratio',action='store',default=None,dest='ratio',help="maxent_id fraud ratio",nargs='+')
    args = parser.parse_args()
    spark = SparkSession.builder.appName("huobi").getOrCreate()
    if args.ratio is not None:
        for ratio in args.ratio:
            print "merge {0} files".format(ratio)
            android_path = "/tmp/qiancheng_android_vld_{0}".format(ratio)
            android_out_file = "/tmp/qiancheng_android_vld_merge_{0}".format(ratio)
            read_file(spark,android_path,android_out_file)

            ios_path = "/tmp/qiancheng_ios_vld_{0}".format(ratio)
            ios_out_file = "/tmp/qiancheng_ios_vld_merge_{0}".format(ratio)
            read_file(spark,ios_path,ios_out_file)

            test_path = "/tmp/qiancheng_sample_new_{0}".format(ratio)
            test_out_file = "/tmp/qiancheng_sample_new_merge_{0}".format(ratio)
            read_file(spark,test_path,test_out_file)
    else:
        dev_path = "/tmp/qiancheng_dev"
        dev_out_path = "/tmp/qiancheng_dev_merge"
        read_file(spark,dev_path,dev_out_path)

