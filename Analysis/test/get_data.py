from __future__ import print_function,division
import json
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
    a.update(record["metrics"][0]["metrics"])
    a.update(record["derived"])
    a["message_timestamp"] = record["content"]["message"]["__timestamp"]
    a["event_id"] = record["content"]["event_id"]
    return a

def getData(path):
    # Define the path of data before run getData
    data = readData(path)
    data = data.filter(lambda x:x['content']['tid'] == 'Q4R5AM9UNIH9RCSYHALWUFOTDKTMDT8H').map(transData)
    df = spark.createDataFrame(data, samplingRatio=1)
    df = df.repartition(10)
    df.write.format("parquet").mode("overwrite").save("/tmp/huobi")
    return data

if __name__ == '__main__':
    path = "/flume/original_scorer_input_data/day=2017080[2-9]"
    # path = "/flume/original_scorer_input_data/day=20170520"
    data = getData(path=path)
