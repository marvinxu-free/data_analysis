# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to give a stastic report for jiufu
but used huanbei data
"""
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from maxent_dp.read_data import read_data_by_days
import re
from maxent_dp.time_utils import get_day_range
import json


def get_huanbei_data_week(sc, path, start, end, week_index, save_path):
    """

    :param sc:
    :param path:
    :param start:
    :param end:
    :param save_path:
    :return:
    """
    p = re.compile(r'(.*)("aid":")(.*?)(".*}$)')
    data = read_data_by_days(sc=sc, path=path, start=start, end=end)
    data = data.filter(lambda x : ("jwm90s9mnzr033rsew6yz0jxwqal7xom" in x)
                                  and ("""\"aid\":\"""" in x))
    data = data.map(lambda x: p.sub('\g<1>"ckid":"\g<3>{0}\g<4>'.format("a" * week_index), x))
    week_save_path = "{0}/week{1}".format(save_path, week_index)
    data.saveAsTextFile(path=week_save_path)


def get_huanbei_data(sc, path, start, end, save_path):
    data_path = "{0}/all".format(save_path)
    csv_path = "{0}/csv".format(save_path)
    try:
        data = sc.textFile(data_path)
        print "read data already exist"
    except Exception:
        day_list = get_day_range(start=start, end=end)
        week_list = [day_list[i:i+7] for i  in range(0, len(day_list), 7)]
        for i, week in enumerate(week_list):
            print "read week {0} from {1} to {2}".format(i, week[0], week[-1])
            get_huanbei_data_week(sc=sc, path=path,start=week[0], end=week[-1], week_index=i, save_path=save_path)
        print "merge all week datas together"
        data = sc.textFile("{0}/week[0-9]*".format(save_path))
        data.saveAsTextFile(path=data_path)
    data_json = data.map(json.loads).map(lambda x: transfer(x))
    df = spark.createDataFrame(data_json, samplingRatio=1)
    df.write.mode("overwrite").option("header", "true").csv(csv_path)


def transfer(row):
    a = {}
    a['timestamp'] = row['content'].get("timestamp")
    a['type'] = row['content'].get("type", None)
    a['ckid'] = row['content']['did'].get("ckid", None)
    a['ip'] = row['content'].get("ip", None)
    a['event_id'] = row['content'].get("event_id", None)
    a['country'] = row['content']['ip_geo'].get("country", None)
    a['country_code'] = row['content']['ip_geo'].get("country_code", None)
    a['province'] = row['content']['ip_geo'].get("province", None)
    a['city'] = row['content']['ip_geo'].get("city", None)
    a['is_cracked'] = row['content']["features"].get("is_cracked","UNKNOWN")
    a['ua_mismatch'] = row['content']["features"].get("ua_mismatch","UNKNOWN")
    a['is_proxy'] = row['content']["features"].get("is_proxy","UNKNOWN")
    a['is_simulator'] = row['content']["features"].get("is_simulator","UNKNOWN")
    if row['content']["message"].get('__fields') is not None:
        a['mobile'] = row['content']["message"]['__fields'].get("mobile", None)
    else:
        a['mobile'] = None
    return a


if __name__ == '__main__':
    path = "/flume/event_maxentid"
    save_path = "/tmp/marvin/dfp_huanbei"
    spark = SparkSession.builder.appName("fumi").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true"
    )
    get_huanbei_data(sc=sc, path=path, start="20171010", end="20171207", save_path=save_path)

