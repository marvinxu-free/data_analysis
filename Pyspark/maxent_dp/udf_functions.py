# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/9
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pyspark.sql.functions import lit,col,udf,count
from pyspark.sql.types import *


def udf_string_int(data_str):
    if data_str == 'false':
        return 0
    elif data_str == 'true':
        return 1
    else:
        return 0


def udf_proxy_ua(ua,proxy):
    if ua == 'true' or proxy == 'true':
        return 1
    else:
        return 0


def udf_all(df):
    """
    this function use udf before and convert some string columns to integer
    :param df:
    :return:
    """
    udf_function = udf(udf_string_int,IntegerType())
    if df.select("cracked_value").dtypes[0][1] == 'string':
        df = df.withColumn('crack_bak',udf_function("cracked_value")) \
            .drop("cracked_value") \
            .withColumnRenamed('crack_bak',"cracked_value")
    if df.select("idcIP_value").dtypes[0][1] == 'string':
        df = df.withColumn('idIP_bak',udf_function("idcIP_value")) \
            .drop("idcIP_value") \
            .withColumnRenamed('idIP_bak',"idcIP_value")

    udf_ua_functions = udf(udf_proxy_ua,IntegerType())
    df = df.withColumn('proxy_ua', \
                       udf_ua_functions("uaMismatch_value","proxyIP_value")) \
        .drop("proxyIP_value") \
        .drop("uaMismatch_value")
    return df


def udf_encrypt_imei(imei):
    """
    udf function for imei encryption, change last five chars to head
    :param imei:
    :return:
    """
    if imei is not None:
        imes_new = imei[-5:] + imei[:-5]
    else:
        imes_new = imei
    return imes_new


def encrypt_imeis(df):
    """
    this function used to encrypt imeis, swap last five chars to the head
    :param df:
    :return: df_new
    """
    udf_function = udf(udf_encrypt_imei,StringType())
    df = df.withColumn('imei_new', \
                       udf_function("imei")) \
        .drop("imei") \
        .withColumnRenamed('imei_new',"imei")
    return df
