# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to give a stastic report for huanbei based on jiufu report
add dfp detail
aid里面存在"k\\u007F8ow6",或导致re.sub失败， 但是本地可以成功
故而这里filter的时候对aid只选取[0-9,a-z,A-Z]
"""
from pyspark.sql import SparkSession, Row
from maxent_dp.read_data import read_data_by_days
from pyspark import StorageLevel
import re
import json
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def get_huanbei_data_dfp(sc, path, start, end, save_path):
    """
    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    csv_path = "{0}_csv".format(save_path)
    p1 = re.compile(r'(.*)("aid":")([0-9,a-z,A-Z]+)(".*}$)')
    p2 = re.compile(r'(.*)("maxent_id":")(.*?)(".*}$)')

    def get_new_maxent_id(x):
        g1 = p1.search(x)
        aid_str = g1.groups()[2]
        x_new = p2.sub("\g<1>\g<2>{0}\g<4>".format(aid_str.encode('utf-8')), x)
        return x_new

    data = read_data_by_days(sc=sc, path=path, start=start, end=end)
    data = data.filter(lambda x: (u"jwm90s9mnzr033rsew6yz0jxwqal7xom" in x)
                                 and (p1.search(x) is not None))
    data = data.map(lambda x: get_new_maxent_id(x))
    data.saveAsTextFile(path=save_path)
    data_json = data.map(json.loads).map(lambda x: transfer(x))
    df = spark.createDataFrame(data_json, samplingRatio=1)
    print "save csv to {0}".format(csv_path)
    df.write.mode("overwrite").option("header", "true").csv(csv_path)


def transfer(row):
    a = {}
    a['timestamp'] = row['content'].get("timestamp")
    a['maxent_id'] = row['content'].get("maxent_id", None)
    a['type'] = row['content'].get("type", None)
    a['ip'] = row['content'].get("ip", None)
    a['event_id'] = row['content'].get("event_id", None)
    a['country'] = row['content']['ip_geo'].get("country", None)
    a['country_code'] = row['content']['ip_geo'].get("country_code", None)
    a['province'] = row['content']['ip_geo'].get("province", None)
    a['city'] = row['content']['ip_geo'].get("city", None)
    a['is_cracked'] = row['content']["features"].get("is_cracked", "UNKNOWN")
    a['ua_mismatch'] = row['content']["features"].get("ua_mismatch", "UNKNOWN")
    a['is_proxy'] = row['content']["features"].get("is_proxy", "UNKNOWN")
    a['is_simulator'] = row['content']["features"].get("is_simulator", "UNKNOWN")
    if row['content']["message"].get('__fields') is not None:
        a['mobile'] = row['content']["message"]['__fields'].get("mobile", None)
    else:
        a['mobile'] = None
    return a


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/huanbei_similar"
    spark = SparkSession.builder.appName("fumi").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_huanbei_data_dfp(sc=sc, path=path,
                         start="20171010", end="20171207",
                         save_path=save_path)
