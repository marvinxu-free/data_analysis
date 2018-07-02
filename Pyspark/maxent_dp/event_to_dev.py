# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to transfer event to deivce
"""

from pyspark.sql.functions import lit,col,count
from pyspark.sql.functions import max,min,mean
from pyspark import StorageLevel
import re


def convert_event_dev(df):
    cols = df.columns
    print("all cols is {0}".format(cols))
    anomaly_pattern = re.compile(r'.*anomaly$')
    anomaly_cols = filter(lambda x:anomaly_pattern.match(x),cols)
    value_pattern = re.compile(r'.*value$')
    value_cols = filter(lambda x:value_pattern.match(x),cols)
    count_pattern = re.compile(r'.*counts$')
    count_cols = filter(lambda x:count_pattern.match(x),cols)
    print("count cols is {0}".format(count_cols))

    loan_pattern = re.compile(r'.*loan$')
    loan_cols = filter(lambda x:loan_pattern.match(x),cols)
    print("schema is :\n{0}".format(df.printSchema()))
    df = df.fillna(1.0, subset=anomaly_cols)
    df = df.fillna(1.0, subset=value_cols)
    df = df.fillna(0.0, subset=loan_cols)
    df = df.fillna(0.0, subset=count_cols)
    df = df.fillna(0.0, subset=['label'])
    event_num_agg = [count('event_id').alias('event_num')]
    label_ua_agg = [max('label').alias('label'),max('proxy_ua').alias('proxy_ua')]
    anomaly_agg = [max(a).alias(a) for a in anomaly_cols]
    loan_agg = [max(a).alias(a) for a in loan_cols]
    value_agg = [mean(a).alias(a) for a in value_cols]
    count_agg = [mean(a).alias(a) for a in count_cols]
    agg_expr = event_num_agg + label_ua_agg + anomaly_agg + loan_agg + value_agg + count_agg
    df_final = df.groupby(['maxent_id','os']).agg(*agg_expr)
    df_final.persist(StorageLevel.DISK_ONLY)
    df_final_num = df_final.count()
    print "final grouped num is {0}".format(df_final_num)
    return df_final
