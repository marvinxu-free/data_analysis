# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
根据brand_anornaly.csv里面的数据获取对应的异常事件
"""
from pyspark.sql import SparkSession, Row
from maxent_dp.read_data import read_data_by_days
import re
import os

def get_huanbei_data_dfp(sc, path, start, end, save_path):
    """
    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    brand_file = "{0}/brand_anormaly.csv".format(dir_path)
    anormal_path = "{0}_anormal".format(save_path)
    with open(brand_file, "r") as f:
        anormal_devices = f.readlines()
    anormal_devices = [x.strip() for x in anormal_devices]

    p = re.compile(r'(.*)("maxent_id":")([a-z,0-9]+)(".*}$)')
    def filter_device(data):
        g = p.search(data)
        if g.groups()[2] in anormal_devices:
            return True
        else:
            return False
    data = read_data_by_days(sc=sc, path=path, start=start, end=end)
    data.filter(lambda x: filter_device(x))
    data.saveAsTextFile(path=anormal_path)


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/device_anormaly"
    spark = SparkSession.builder.appName("jiufu").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_huanbei_data_dfp(sc=sc, path=path,
                         start="20171211", end="20171217",
                         save_path=save_path)

