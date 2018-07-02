# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file is used to get uniq imei from beta during 0801-0820
which label 1 : label 0 = 1: 10
"""

from pyspark.sql import SparkSession
from maxent_dp.get_imei import get_imei_for_qiaoda


if __name__ == "__main__":
    path = "/flume/beta/offline/tid=79rctxsz5nkuc9dd9qax3ewf5hay691k"
    save_path = "/tmp/qiaoda_cr_imei"
    spark = SparkSession.builder.appName("qiancheng").enableHiveSupport().getOrCreate()
    # get_imei_for_qiaoda(path=path,spark=spark,save_path=save_path)
    get_imei_for_qiaoda(fname=path,save_path=save_path,spark=spark)

