# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/17
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this filed used to do some join operation
"""
from pyspark.sql.functions import lit, col


def join_sub(df,sub,col_name):
    df_new = df.join(sub,col_name,'inner')
    return df_new


def join_state(df1,df2,col_name):
    df2 = df2.withColumn("label", lit(1))
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new


def join_loan(df1,df2,col_name='maxent_id'):
    df_new = df1.join(df2, col_name, "left_outer")
    return df_new


def join_leftanti(df1, df2, col_name):
    df_new = df1.join(df2, col_name, "leftanti")
    return df_new


join_inner = join_sub
join_left_outer = join_loan
