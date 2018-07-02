import numpy as np
import pandas as pd
import os
import json
os.environ["SPARK_HOME"] = "/Users/chaoxu/software/spark-2.1.1-bin-hadoop2.7"
from operator import add
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from Pic.check_model import scored


def transAnomaly(record):
    a = record.copy()
    score = 0.0
    num = 0
    for i in record:
        if "anomaly" in i:
            num += 1
            _log = np.log(record[i]) if record[i] is not None else 0.0
            score += _log
            a["%s.log" % i] = _log
    a["score"] = scored(score,num)
    return a

def getDataFrame(path, spark):
    # Define the path of data before use getDataFrame.
    data = spark.read.parquet(path)
    data = data.rdd.map(lambda x: x.asDict())
    re = data.collect()
    re = list(map(transAnomaly, re))
    df = pd.DataFrame.from_dict(re, orient="columns")
    return df

def mergeProxy(a, b):
    if a == "true" or b == "true":
        return "true"
    else:
        return "false"
if __name__ == "__main__":
    path = "/Users/chaoxu/code/local-spark/Data/huobi"
    save_path = "/Users/chaoxu/code/local-spark/Data"
    spark = SparkSession.builder.appName("huobi").getOrCreate()
    df = getDataFrame(path, spark)
    # choose valid data
    df["mergeProxy"] = df.apply(lambda x: mergeProxy(x["proxyIP.value"], x["uaMismatch.value"]), axis=1)
    cols = df.columns.values.tolist()
    print cols , "\n"
    cols = [cols.pop(cols.index("score"))] + cols
    print cols
    df = df[cols]
    df.to_csv(save_path + '/huobi.csv', encoding='utf-8')
