# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/8/15
# Company : Maxent
# Email: chao.xu@maxent-inc.com

import os
os.environ["SPARK_HOME"] = "/Users/chaoxu/software/spark-2.1.1-bin-hadoop2.7"

from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile("./render.html", 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
    sc.stop()