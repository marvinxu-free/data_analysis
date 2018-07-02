import numpy as np
import pandas as pd

def transAnomaly(record):
    a = record.copy()
    score = 0.0
    for i in record:
        if "anomaly" in i:
            _log = np.log(record[i]) if record[i] is not None else 0.0
            score += _log
            a["%s.log" % i] = _log
    a["score"] = score
    return a

def getDataFrame(path):
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

path = "/Users/chaoxu/sample_data/original"
df = getDataFrame(path)
# choose valid data
df = df[df["message_timestamp"] > 1489075200000.0]
df["mergeProxy"] = df.apply(lambda x: mergeProxy(x["proxyIP.value"], x["uaMismatch.value"]), axis=1)

df.to_csv('/Users/chaoxu/Documents/maxent/python/henyuan/data/local_csv.csv')
# cols = list(filter(lambda x: "log" in x, df.columns)) + ["score"]
# for i in cols:
#     makeMosaic(df[i], df["cracked.value"],df)
#
# for i in cols:
#     makeMosaic(df[i], df["mergeProxy"],df)

# for i in cols:
#     makeHist(i,df=df)
# valueAnomalyMosaic("ipSeg24.1h.value", "ipSeg24.1h.anomaly.log",df=df)
#
# #unormal
# ipSeg24Curve("59.41.95",df)
# ipSeg24Curve("59.41.94",df)
# ipSeg24Curve("59.42.128",df)
# ipSeg24Curve("59.42.129",df)
#
# #normal
# ipSeg24Curve("117.136.79",df)
# df[df["score"] > 10]["ipSeg24"].unique()
# makeHist("score",df)