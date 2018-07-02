# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to give a stastic report for liuqu
"""
from pyspark.sql import SparkSession, Row
from pyspark import StorageLevel
from maxent_dp.read_data import read_data_by_days
from maxent_dp.hdfs_shell import chk_dir_exist
from maxent_dp.col_map import common_map_cols
import re
import json
from copy import deepcopy


ios_map_cols = {
    u'广告标识符': 'idfa',
    u'Cookie ID': 'ckid',
}

android_map_cols = {
    u'广告标识符': 'aid',
    u'国际移动装备标识符': 'imei',
    u'物理网卡地址': 'mac',
    u'Cookie ID': 'ckid',
}


def get_liuqu_data_dfp(sc, path, start, end, os_name, save_path):
    """

    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    csv_path = "{0}/{1}_csv".format(save_path, os_name)
    os_save_path = "{0}/{1}".format(save_path, os_name)
    map_cols = deepcopy(common_map_cols)
    if os_name == 'ios':
        map_cols.update(ios_map_cols)
    else:
        map_cols.update(android_map_cols)

    if chk_dir_exist(os_save_path):
        data = sc.textFile(os_save_path)
        data_json = data.map(lambda x: transfer(x, map_cols))
        data_json.persist(StorageLevel.DISK_ONLY)
        data_num = data_json.count()
        print "data num is {0}".format(data_num)
        # data_json = data.map(json.loads).map(lambda x: transfer(x, map_cols, r))
        # df = data_json.map(lambda x: Row(x)).toDF()
        df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").option("nullValue"," ").csv(csv_path)
    else:
        data = read_data_by_days(sc=sc, path=path, start=start, end=end)
        data = data.filter(lambda x: "7b249443ea633971a572c6d0a5d26932" in x and '"os":"{0}"'.format(os_name) in x)
        data.persist(StorageLevel.DISK_ONLY)
        data.saveAsTextFile(path=os_save_path)
        data_json = data.map(lambda x: transfer(x, map_cols))
        # df = data_json.map(lambda x: Row(x)).toDF()
        df = spark.createDataFrame(data_json, samplingRatio=1)
        print "save csv to {0}".format(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)


def transfer(row_str, map_cols):
    if isinstance(row_str, str):
        row_str = row_str.decode('utf-8')
    a = {}
    for k, v in map_cols.iteritems():
        # print "to match {0}".format(v)
        r = re.compile(ur"""("{0}":)(".*?"|[\w]+)""".format(v))
        g = r.search(row_str)
        if g:
            value = g.groups()[1].replace('"','')
        else:
            value = ""
        a[v] = value
    return a


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/liuqu_state"
    spark = SparkSession.builder.appName("liuqu").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    # get_liuqu_data_dfp(sc=sc, path=path,
    #                    start="20180328", end="20180402",
    #                    os_name='ios',
    #                    save_path=save_path)
    get_liuqu_data_dfp(sc=sc, path=path,
                       start="20180328", end="20180402",
                       os_name='android',
                       save_path=save_path)
