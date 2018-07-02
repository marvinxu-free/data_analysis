# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/28
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本代码用于画Documnet/huanqiu_heika/环球黑卡异常度fenxi.md中插图
"""
from __future__ import division, print_function
import pandas as pd
from Pic.check_model import scored
from Utils.common.patterns import *
from Utils.common.custerReadFile import read_multi_csv
from Pic.hist import anormalyScoreHist
from Pic.hist import makeSFeatureHist
from Pic.hist import makeHist
from Pic.mosic import valueAnomalyMosaic
from Pic.scoreTime import scoreVsTime
from Params.path_params import *
import errno
from datetime import timedelta


def transAnomaly(df):
    anormaly_cols = df.columns.values[anormaly_match(df.columns.values)]
    anormaly_log_cols = map(lambda x: "{0}.log".format(x), anormaly_cols)
    for x_log, x in zip(anormaly_log_cols, anormaly_cols):
        df[x_log] = df[x].apply(lambda x: np.log(x) if x else 0.0)
    df['score'] = df[anormaly_log_cols].sum(axis=1)
    col_num = len(anormaly_log_cols)
    df['score'] = df['score'].apply(lambda x: scored(x, col_num))
    return df


def read_data(path, tmp_file):
    """
    read ,transfer data and pic
    :param path:
    :return:
    """
    try:
        df = pd.read_csv(tmp_file)
    except Exception as e:
        df = read_multi_csv(path=path)
        df = transAnomaly(df)
        df.to_csv(tmp_file, index=False)
    return df


def pic_heika(df, save_path):
    df["timestamp"] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df["timestamp"] = pd.DatetimeIndex(df["timestamp"]) + timedelta(hours=8)
    df = df.sort_values(by="timestamp")

    print("image path {0}".format(save_path))
    if not os.path.exists(save_path):
        try:
            os.makedirs(save_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise

    # ip_vcols = df.columns.values[ipSeg_vmatch(df.columns.values)]
    ip_alcols = df.columns.values[ipSeg_almatch(df.columns.values)]
    # ipGeo_vcols = df.columns.values[ipGeo_vmatch(df.columns.values)]
    ipGeo_alcols = df.columns.values[ipGeo_almatch(df.columns.values)]
    # did_vcols = df.columns.values[did_vmatch(df.columns.values)]
    did_alcols = df.columns.values[did_almatch(df.columns.values)]
    # maxentID_vcols = df.columns.values[maxenID_vmatch(df.columns.values)]
    maxentID_alcols = df.columns.values[maxenID_almatch(df.columns.values)]
    anormaly_l_cols = df.columns.values[anormaly_l_match(df.columns.values)]
    # scoreVsTime(df=df, path=save_path)

    # #get score count hist
    # anormalyScoreHist(cols=anormaly_l_cols, score='score', df=df, path=save_path)

    for j in ip_alcols:
        makeHist(j, df, path=save_path)
    for j in ipGeo_alcols:
        makeHist(j, df, path=save_path)
    for j in maxentID_alcols:
        makeHist(j, df, path=save_path)
    for j in did_alcols:
        makeHist(j, df, path=save_path)

        # for (i, j) in zip(ip_vcols,ip_alcols):
        #     valueAnomalyMosaic(i,j, df=df, path=save_path)
        # for (i, j) in zip(ipGeo_vcols,ipGeo_alcols):
        #     valueAnomalyMosaic(i,j, df=df, path=save_path)
        # for (i, j) in zip(maxentID_vcols,maxentID_alcols):
        #     valueAnomalyMosaic(i,j, df=df, path=save_path)
        # for (i, j) in zip(did_vcols,did_alcols):
        #     valueAnomalyMosaic(i,j, df=df, path=save_path)

if __name__ == '__main__':
    heika_data_path = "{0}/huanqiu_heika".format(Data_path)
    data_dir = "{0}/huanqiu_heika_anormaly_csv".format(heika_data_path)
    data_tmp_file = "{0}/tmp/tmp.csv".format(heika_data_path)
    heika_img = "{0}/img_heika".format(Data_path)
    df = read_data(path=data_dir, tmp_file=data_tmp_file)
    pic_heika(df=df, save_path=heika_img)


