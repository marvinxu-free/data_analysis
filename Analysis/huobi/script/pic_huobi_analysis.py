import pandas as pd
from datetime import timedelta
import os
import sys
from datetime import timedelta

import pandas as pd

from Pic.hist import anormalyScoreHist
from Pic.hist import makeSFeatureHist
from Pic.scoreTime import scoreVsTime
from Utils.common.patterns import *

reload(sys)
sys.setdefaultencoding('utf8')


def mergeProxy(a, b):
    if a == "true" or b == "true":
        return "true"
    else:
        return "false"


def reduce_func(x, y):
    if x == 1 or y == 1:
        return 1
    return x + y


if __name__ == "__main__":
    path = '/Users/chaoxu/code/local-spark/Data/huobi.csv'
    save_path = os.path.split(path)[0] + "/image_huobi"
    df = pd.read_csv(path)
    cols = df.columns.values[1:]
    df = df[cols]
    # time scope
    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df["timestamp"] = pd.DatetimeIndex(df["timestamp"]) + timedelta(hours=8)
    df = df.sort_values(by="timestamp")
    # df = df.loc[(df["timestamp"] >= "20170807") & (df["timestamp"] < "20170810")]

    ip_vcols = df.columns.values[ipSeg_vmatch(df.columns.values)]
    ip_alcols = df.columns.values[ipSeg_almatch(df.columns.values)]
    ipGeo_vcols = df.columns.values[ipGeo_vmatch(df.columns.values)]
    ipGeo_alcols = df.columns.values[ipGeo_almatch(df.columns.values)]
    did_vcols = df.columns.values[did_vmatch(df.columns.values)]
    did_alcols = df.columns.values[did_almatch(df.columns.values)]
    maxentID_vcols = df.columns.values[maxenID_vmatch(df.columns.values)]
    maxentID_alcols = df.columns.values[maxenID_almatch(df.columns.values)]
    anormaly_cols = df.columns.values[anormaly_match(df.columns.values)]

    # draw fraud type'Classp'
    # fraudType(df=df, path=save_path,title="normal fraud type")

    # draw score
    scoreVsTime(df=df, path=save_path)

    # #get score count hist
    anormalyScoreHist(cols=anormaly_cols, score='score', df=df, path=save_path)

    # for (i, j) in zip(ip_vcols,ip_alcols):
    #     makeHist(i,df,path=save_path)
    #     makeHist(j,df,path=save_path)
    # for (i, j) in zip(ipGeo_vcols,ipGeo_alcols):
    #     makeHist(i,df,path=save_path)
    #     makeHist(j,df,path=save_path)
    # for (i, j) in zip(maxentID_vcols,maxentID_alcols):
    #     makeHist(i,df,path=save_path)
    #     makeHist(j,df,path=save_path)
    # for (i, j) in zip(did_vcols,did_alcols):
    #     makeHist(i,df,path=save_path)
    #     makeHist(j,df,path=save_path)

    # for (i, j) in zip(ip_vcols,ip_alcols):
    #     valueAnomalyMosaic(i,j, df=df, path=save_path)
    # for (i, j) in zip(ipGeo_vcols,ipGeo_alcols):
    #     valueAnomalyMosaic(i,j, df=df, path=save_path)
    # for (i, j) in zip(maxentID_vcols,maxentID_alcols):
    #     valueAnomalyMosaic(i,j, df=df, path=save_path)
    # for (i, j) in zip(did_vcols,did_alcols):
    #     valueAnomalyMosaic(i,j, df=df, path=save_path)
    #
    # path1 = '/Users/chaoxu/code/local-spark/Data/zip_lzip.csv'
    # df1 = pd.read_csv(path1)

    # path2 = '/Users/chaoxu/code/local-spark/Data/zip_szip.csv'
    # df2 = pd.read_csv(path2)
    # check_model(df,df2,feature="maxentID", interval="7d",short=True)

    # event type
    # eventType(df=df, path=save_path,title=" normal event type")
    # eventType(df=df, path=save_path,title=" suspect event type", _type="suspect")
    # eventType(df=df, path=save_path,title=" fraud event type", _type="fraud")
    #

    # for (i, j) in zip(ipGeo_vcols,ipGeo_alcols):
    #     makeFeatureHist(i,j,df,feature='ipGeo',scope=[6,8],path=save_path)
    for (i, j) in zip(maxentID_vcols, maxentID_alcols):
        makeSFeatureHist(i, j, df, feature='maxentID', scope=[6, 8], path=save_path)
