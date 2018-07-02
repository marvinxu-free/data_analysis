# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/3/5
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
用于萨摩耶数据不匹配分析，
主要步骤：
1. 提取20180101-20180227时间段萨摩耶数据
2. 分析出发点：
   a. aid是谷歌给出的广告识别码，可变但是不重合。存在多对一，但是不存在一对多的问题。因此重点检查一对多的问题。
   b。 idfa类似aid， 也是主要检查多对一的问题；但是idfa在ios xx版本后不可取，变成了一个不变的值，这个也需要额外注意。
3. 根据萨摩耶给的Excel，主要关注的aid：558ac8d3bbc773fc, 对应4个maxent_id
4. 根据萨摩耶给出的Excel，主要关注的idfa如下：19F68219-1C19-4E63-95DB-A23D297A5806, 对应4个maxent_id
"""
from pyspark.sql import SparkSession, Row
from maxent_dp.read_data import read_data_by_days
import re
import os


def filter_device(data, p, match_value):
    g = p.search(data)
    if g:
        if g.groups()[2] == match_value:
            return True
        else:
            return False
    else:
        return False


def get_data(sc, data_path, start, end, save_path):
    all_data = "{0}/all".format(save_path)
    ios_path = "{0}/ios".format(save_path)
    android_path = "{0}/android".format(save_path)
    p_android = re.compile(r'(.*)("aid":")([a-z,0-9, A-Z]+?)(".*}$)')
    p_ios = re.compile(r'(.*)("idfa":")([a-z,0-9, A-Z]+?)(".*}$)')
    # day_paths = map(lambda x: "{0}/day={1}".format(path, x), day_list)
    # day_paths_str = ",".join(day_paths)
    try:
        data = sc.textFile(all_data)
        data_ios = data.filter(lambda x: '19F68219-1C19-4E63-95DB-A23D297A5806' in x)
        data_android = data.filter(lambda x: 'a46b7d8e322ae199' in x)
        data_ios.saveAsTextFile(path=ios_path)
        data_android.saveAsTextFile(path=android_path)
    except Exception:
        data = read_data_by_days(sc=sc, path=data_path, start=start, end=end)
        data = data.filter(lambda x: 'c8a4db5fa1b93b7fb9f31b815a6cf611' in x or 'c0155306e4aa3138a83143ff2b5685bd' in x)
        data.saveAsTextFile(path=all_data)
        print "read all data done"
        data_ios = data.filter(lambda x: '19F68219-1C19-4E63-95DB-A23D297A5806' in x)
        data_android = data.filter(lambda x: 'a46b7d8e322ae199' in x)
        data_ios.saveAsTextFile(path=ios_path)
        data_android.saveAsTextFile(path=android_path)


def get_data_base_mxid(sc, data_path, start, end, save_path):
    all_data = "{0}/all".format(save_path)
    ios_path = "{0}/ios_mxid".format(save_path)
    android_path = "{0}/android_mxid".format(save_path)
    # print "get data from {0} to {1}".format(day_list[0], day_list[-1])
    # day_paths = map(lambda x: "{0}/day={1}".format(path, x), day_list)
    # day_paths_str = ",".join(day_paths)
    try:
        data = sc.textFile(all_data)
        data_ios = data.filter(lambda x: '083a8ed9b670bcf544166602ba9b68ce' in x)
        data_android = data.filter(lambda x: '00b7858e07aa434351de961019e71190' in x)
        data_ios.saveAsTextFile(path=ios_path)
        data_android.saveAsTextFile(path=android_path)
    except Exception:
        data = read_data_by_days(sc=sc, path=data_path, start=start, end=end)
        data = data.filter(lambda x: 'c8a4db5fa1b93b7fb9f31b815a6cf611' in x or 'c0155306e4aa3138a83143ff2b5685bd' in x)
        data.saveAsTextFile(path=all_data)
        print "read all data done"
        data_ios = data.filter(lambda x: '083a8ed9b670bcf544166602ba9b68ce' in x)
        data_android = data.filter(lambda x: '00b7858e07aa434351de961019e71190' in x)
        data_ios.saveAsTextFile(path=ios_path)
        data_android.saveAsTextFile(path=android_path)


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/samoyed"
    spark = SparkSession.builder.appName("samoyed").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_data(sc=sc,
             data_path=path,
             start='20180101',
             end='20180227',
             save_path=save_path)
    # get_data_base_mxid(sc=sc,
    # data_path=path,
    #                    start='20180101',
    #                    end='20180227',
    #                    save_path=save_path)
