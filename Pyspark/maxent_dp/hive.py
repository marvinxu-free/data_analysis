# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/11/17
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to do some hive operation
"""


def get_label_maxentid(spark):
    """
    get qiancheng fraud maxent_id v2
    :param spark:
    :return:
    """
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud_new""")
    return mdf


def get_label_v1_maxentid(spark):
    """
    get qiancheng fraud maxent_id v1
    :param spark:
    :return:
    """
    mdf = spark.sql("""select maxent_id from odata.qiancheng_fraud""")
    return mdf


def get_vld_ios(spark,path='/tmp/qiancheng_ios_vld'):
    ios_vld = spark.sql("""select maxent_id from odata.qiancheng_ios_fraud""")
    return ios_vld


def get_vld_android(spark,path='/tmp/qiancheng_android_vld'):
    android_vld = spark.sql("""select maxent_id from odata.qiancheng_android_fraud""")
    return android_vld


def get_qiancheng_android_pred(spark):
    """
    get maxent pred andriod fraud maxent_ids, num 442657
    :param spark:
    :return:
    """
    android_pred = spark.sql("""select maxent_id from odata.qiancheng_android_pred""")
    return android_pred


def get_qiancheng_ios_pred(spark):
    """
    get maxent pred ios fraud maxent_ids, num 141188
    :param spark:
    :return:
    """
    ios_pred = spark.sql("""select maxent_id from odata.qiancheng_ios_pred""")
    return ios_pred


def get_tid_upload(spark, tid, start, end):
    """
    state tid from fr_analysis.log_id_system_v2_call_original
    sql:
    select count(*) from fr_analysis.log_id_system_v2_call_original
    where get_json_object(json, '$.tid') = "5g0hau6ige6ndtwx6fg9a9gyxbhcgjg4"
    and day >= 20171128 and day <= 20171204;

    :param spark:
    :param tid:
    :return:
    """
    sql = \
        """
        select count(*)
        from fr_analysis.log_id_system_v2_call_original
        where get_json_object(json, '$.tid') = '{0}' and day >= {1} and day <= {2} 
        """.format(tid, start, end)
    print("sql is {0}".format(sql))
    tid_num = spark.sql(sql).count()
    print("get tid num from {0} is {1}".format("fr_analysis.log_id_system_v2_call_original", tid_num))
    return tid_num


