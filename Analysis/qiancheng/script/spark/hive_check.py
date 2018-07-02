# -*- coding: utf-8 -*-
# Project: maxent-ml
# Author: chaoxu create this file
# Time: 2017/9/4
# Company : Maxent
# Email: chao.xu@maxent-inc.com
# this file is used to run spark local
from pyspark.sql import SparkSession
from pyspark import StorageLevel


def get_hive(spark):
    df = spark.sql("""
SELECT DISTINCT(c.maxent_id) FROM
(select get_json_object(json, '$.maxent_id') as maxent_id
from fr_analysis.event_maxentid_original
where day>=20170801 and day<= 20170820 and get_json_object(json, '$.tid') = '79rctxsz5nkuc9dd9qax3ewf5hay691k' ) c
""")
    return df

def get_label_maxentid(spark):
    # mdf = spark.sparkContext.textFile(path)
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud_new""")
    return mdf


if __name__ == "__main__":
    spark = SparkSession.builder.appName("hive-check").enableHiveSupport().getOrCreate()
    save_path = "/tmp/hive-check"
    hive_df = get_hive(spark=spark)
    hive_df.persist(StorageLevel.DISK_ONLY)
    hive_num = hive_df.count()
    print "get hive data num is {0}".format(hive_num)

    maxent_ids = get_label_maxentid(spark=spark)
    maxent_ids.persist(StorageLevel.DISK_ONLY)
    maxent_ids_num = maxent_ids.count()
    print "get maxent_ids num is {0}".format(maxent_ids_num)

    sub_df = maxent_ids.subtract(hive_df)
    sub_df.persist(StorageLevel.DISK_ONLY)
    sub_df_num = sub_df.count()
    print "missing maxent_id num is {0}".format(sub_df_num)
    sub_df.coalesce(1).write.mode("overwrite").option("header","false").csv(save_path)
