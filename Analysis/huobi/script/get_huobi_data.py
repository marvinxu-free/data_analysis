from __future__ import print_function,division
import json
import re
value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')
def readData(path):
    """
    :param path: hdfs file path 
    :return: data
    """
    data = sc.textFile(path)
    data = data.map(json.loads)
    data = data.repartition(100)
    data.persist(StorageLevel.DISK_ONLY)
    return data

def transData(record):
    a = {}
    features = record["features"]
    b = {}
    for (x, y) in features.items():
        value_flg = value_match.match(x)
        anomaly_flg = anomaly_match.match(x)
        if value_flg or anomaly_flg:
            b[x] = y
    derived = {}
    if "ipGeo" in record["derived"]:
        derived["ipGeo"] = record["derived"]['ipGeo']
    if "ipSeg24" in record["derived"]:
        derived["ipSeg24"] = record["derived"]['ipSeg24']
    a.update(b)
    a.update(derived)
    a["message_timestamp"] = record["content"]["message"]["__timestamp"]
    a["timestamp"] = record["content"]["timestamp"]
    a["event_id"] = record["content"]["event_id"]
    a["event_type"] = record["event_type"]
    return a

def getData(path):
    # Define the path of data before run getData
    data = readData(path)
    data = data.filter(lambda x:x['content']['tid'] == 'Q4R5AM9UNIH9RCSYHALWUFOTDKTMDT8H')\
        .filter(lambda x:x['event_type'] == "Transaction" or x['event_type'] == "CreateAccount") \
        .map(transData)
        # .filter(lambda x:x["features"]['ipGeo.1d.value'] >=6500 and x["features"]['ipGeo.1d.anomaly'] == 1000)\
    df = spark.createDataFrame(data, samplingRatio=1)
    df = df.repartition(10)
    df.write.format("parquet").mode("overwrite").save("/tmp/huobi")
    return data, df

if __name__ == '__main__':
    # path = "/flume/original_scorer_input_data/day=2017080[7-9]"
    path = "/flume/original_scorer_input_data/day=2017082[5-7]"
    # path = "/flume/original_scorer_input_data/day=20170520"
    data,df = getData(path=path)
