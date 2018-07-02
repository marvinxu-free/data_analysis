import numpy as np
import pandas as pd
from Analysis.Pic.hist import makeHist
from Analysis.Pic.hist import anormalyScoreHist,dataCorr
from Analysis.Pic.mosic import makeMosaic
from Analysis.Pic.mosic import valueAnomalyMosaic
from Analysis.Pic.ipSeg24Curve import ipSeg24Curve
from Analysis.Pic.output_table_macdown import print_macdown_table,print_table

import pandas as pd
import pytablewriter
from tabulate import tabulate
import os
import re

ipSeg_r = re.compile('ipSeg24.*log*')
ipSeg_match = np.vectorize(lambda x:bool(ipSeg_r.match(x)))

anormaly = re.compile('.*anomaly.log$')
anormaly_match = np.vectorize(lambda x:bool(anormaly.match(x)))

ipSeg_v = re.compile('^ipSeg24.*anomaly$')
ipSeg_vmatch = np.vectorize(lambda x:bool(ipSeg_v.match(x)))

maxentID = re.compile('^maxentID.*anomaly$')
maxenID_match = np.vectorize(lambda x:bool(maxentID.match(x)))

ipGeo = re.compile('^ipGeo.*anomaly$')
ipGeo_match = np.vectorize(lambda x:bool(ipGeo.match(x)))


did = re.compile('^did.*anomaly$')
did_match = np.vectorize(lambda x:bool(did.match(x)))
def mergeProxy(a,b):
    if a == "true" or b == "true":
        return "true"
    else:
        return "false"


def reduce_func(x,y):
    if x == 1 or y == 1:
        return 1
    return x + y

path = '/Users/chaoxu/Documents/maxent/python/henyuan/data/local_csv.csv'
save_path = os.path.split(path)[0]
df = pd.read_csv(path)
df = df[df.columns.values[1:]]
ip_cols = df.columns.values[ipSeg_match(df.columns.values)]
ip_vcols = df.columns.values[ipSeg_vmatch(df.columns.values)]
maxID_cols = df.columns.values[maxenID_match(df.columns.values)]
ipGeo_cols = df.columns.values[ipGeo_match(df.columns.values)]
did_cols = df.columns.values[did_match(df.columns.values)]
anormaly_cols = df.columns.values[anormaly_match(df.columns.values)]

# for i,col in enumerate(ip_cols):
#     makeMosaic(df[col], df["cracked.value"],path=save_path)
# #
# for i,col in enumerate(ip_cols):
#     makeMosaic(df[col], df["mergeProxy"],path=save_path)

valueAnomalyMosaic("ipSeg24.1h.value", "ipSeg24.1h.anomaly.log", df=df,path=save_path)


# df_anormaly_7 = df[(df['ipSeg24.1h.value'] > 6) & (df['ipSeg24.1h.anomaly'] == 1)]
# print df_anormaly_7[['ipSeg24.1h.value','ipSeg24.1h.anomaly','ipSeg24.1h.anomaly.log']]
# df_anormaly_7_ip = df_anormaly_7['ipSeg24']
# print 'anormaly bigger than 7 \n',df_anormaly_7_ip , "\n"
#
# df_ipSeg24_count = df['ipSeg24'].value_counts()
#
# for ip in df_anormaly_7_ip:
#     ipSeg24Curve(ip,df,path=save_path)
#
# makeHist("ipSeg24.5m.value",df,path=save_path)
#
# makeHist("ipSeg24.5m.anomaly.log",df,path=save_path)
#
# makeHist("ipSeg24.1h.value",df,path=save_path)
# makeHist("ipSeg24.1h.anomaly.log",df,path=save_path)
#
# makeHist("ipSeg24.1d.value",df,path=save_path)
# makeHist("ipSeg24.1d.anomaly.log",df,path=save_path)
#
# makeHist("score",df,path=save_path)
#
# df_score_gt_7 = df[df['score'] >= 7]
# df_score_gt_36 = df[df['score'] >= 36]
# df_anormaly_gt_36_ip = df_score_gt_36['ipSeg24']
#
# #get score count hist
# anormalyScoreHist(cols=anormaly_cols,score='score',df=df,path=save_path)
#
# #get correlation between cols
# #ipSeg24
# dataCorr(cols=ip_vcols,df=df,path=save_path)
#
# #maxentID
# dataCorr(cols=maxID_cols,df=df,path=save_path,title='maxentID anomaly correlation')
#
# #maxentID
# dataCorr(cols=ipGeo_cols,df=df,path=save_path,title='ipGeo anomaly correlation')
#
# #did
# dataCorr(cols=did_cols,df=df,path=save_path,title='did anomaly correlation')