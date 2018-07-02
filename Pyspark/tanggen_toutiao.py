
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark import StorageLevel
from datetime import date, datetime, timedelta
import pytz


def startSpark():
    """
    """
    spark = SparkSession.builder.appName("huanqiu").getOrCreate()
    return spark


def getUserId(data):
    content = data.get("content", {})
    return content.get("message", {}).get("__seller_user_id", None)


def getItem(data):
    """
    """
    a = {}
    content = data.get("content", {})
    features = data.get("features", {})
    a["tick"] = content.get("tick", None)
    a["type"] = content.get("type", None)
    a["pid"] = content.get("pid", None)
    a["sub_pid"] = content.get("sub_pid", None)
    a["sub_pid_seller"] = content.get("message", {}).get("__seller_user_id", None)
    a["tcpts"] = content.get("tcpts", None)
    a["window"] = content.get("tcp_initial_window", None)
    a["timestamp"] = content.get("timestamp", None)
    tz = pytz.timezone("Asia/Shanghai")
    dt = datetime.fromtimestamp(float(a["timestamp"]) / 1000, tz)
    a["day"] = dt.strftime("%Y-%m-%d")
    a["hour"] = dt.strftime("%H")
    a["maxentID"] = content.get("maxent_id", None)
    a["ipSeg24"] = ".".join(content["ip"].split(".")[:-1])
    a["city"] = content.get("ip_geo", {}).get("city", None)
    a["ipGeo_5m_value"] = features.get("ipGeo.5m.value")
    a["ipSeg24_1h_value"] = features.get("ipSeg24.1h.value")
    a["proxyIP_value"] = features.get("proxyIP.value", "false") == "true"
    a["proxyIP_anomaly"] = float(features.get("proxyIP.anomaly", "1"))
    a["isSimulator_value"] = features.get("uaMismatch.value", "false") == "true"
    #a["isSimulator_value"] = a["isSimulator_value"] | (features.get("isSimulator.value", "false") == "true")
    a["isSimulator_anomaly"] = float(features.get("uaMismatch.anomaly", "1"))\
        + float(features.get("isSimulator.anomaly", "1")) - 1
    if a["isSimulator_value"]:
        a["isSimulator_anomaly"] += np.random.random() * 1 + 2
    else:
        pass
    a["score"] = float(np.log(a["proxyIP_anomaly"]) + np.log(a["isSimulator_anomaly"]))
    for i in features:
        if "anomaly" in i and i not in ["isSimulator.anomaly", "uaMismatch.anomaly", "proxyIP.anomaly",
                "ipSeg24.7d.anomaly", "shopID.1h.anomaly", "shopID.6h.anomaly", "shopID.1d.anomaly",
                "shopID.7d.anomaly"]:
            a["score"] += float(np.log(features[i]))
    a["score"] = float(scoreTrans(a["score"]))
    a["reason"] = ""
    return a


def scoreTrans(score):
    """
    """
    if score <= 0:
        return 0
    low = 10.
    high = 20.
    tmp = (1 - low / score) / (1 - low / high) * (2.2 - 0.85) + 0.85
    return 100 / (np.exp(-1 * tmp) + 1)


data = sc.textFile("/flume/original_scorer_input_data/day=20180405")
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180406"))
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180407"))
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180408"))
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180409"))
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180410"))
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180411"))
data = data.union(sc.textFile("/flume/original_scorer_input_data/day=20180412"))
data = data.filter(lambda x: "d51e44a3684930dfb458a7f1853ef2fc" in x)
data = data.map(json.loads).map(getItem)
data = data.filter(lambda x: x["type"] in ["transaction", "createaccount"])
data = data.repartition(10)
data.persist(StorageLevel.DISK_ONLY)
data.count()

df = data.toDF()
df = df.withColumn("is_fraud", func.when(df.score>90, 1).otherwise(0))
df.groupBy("sub_pid_seller").agg(func.avg("score"), func.count("maxentID"), func.countDistinct("maxentID"), func.sum("is_fraud")).show(2000, truncate=False)



# 找例子

## 5分钟大量操作
df.orderBy("ipGeo_5m_value", ascending=False).select("pid", "ipGeo_5m_value", "city", "score", "tcpts", "maxentid").show()

df.filter("pid='edaijia_93_t_lposter'").orderBy("ipGeo_5m_value", ascending=False).select("pid", "ipGeo_5m_value", "city", "score", "tcpts", "maxentid", "hour").show()

df.filter("pid='jgwg_1219_t_lposter'").groupBy("hour").count().orderBy("hour").show()

## 同一设备反复操作
df.groupBy("maxentid", "hour").count().orderBy("count", ascending=False).show(truncate=False)

df.groupBy("maxentid", "pid").count().orderBy("count", ascending=False).show(truncate=False)

df.filter("maxentid='7f641447b2b208a086578b2382bc0d16'").orderBy("timestamp").select("ipGeo_5m_value", "tcpts", "timestamp", "maxentid").show(200, truncate=False)

df.filter("maxentid='b7e69579daea465ce6111e4cde2bdc40'").orderBy("timestamp").select("ipGeo_5m_value", "tcpts", "timestamp", "maxentid").show(200, truncate=False)

## 单点特征
df = df.withColumn("issimulator", func.when(df.isSimulator_value, 1).otherwise(0))

df.groupBy("pid").agg(func.count("*"), func.sum("issimulator")).orderBy("sum(issimulator)", ascending=False).show(truncate=False)

df.filter("pid='jgwg_1219_t_lposter'").groupBy("ipSeg24").count().orderBy("count", ascending=False).show()

### 城市
df.filter("city!='UNKNOWN'").groupBy("city", "hour").agg(func.count("*"), func.sum("issimulator")).orderBy("sum(issimulator)", ascending=False).show(truncate=False)


### 网段

df.groupBy("ipSeg24", "hour").agg(func.count("*").alias("count")).orderBy("count", ascending=False).show(truncate=False)

df.filter("ipSeg24='218.201.194'").groupBy("ipSeg24", "hour", "day").agg(func.count("*").alias("count")).orderBy("count", ascending=False).show(truncate=False)

df.filter("ipSeg24='218.201.194'").select("hour", "timestamp").orderBy("timestamp").show()