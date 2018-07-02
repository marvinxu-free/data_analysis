# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/10
# Company : Maxent
# Email: chao.xu@maxent-inc.com
import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000
from Params.path_params import Data_path
from Utils.common.custerReadFile import read_multi_csv
import os
import errno
import sys
from device_behavior import pic_device_behavior
from id_anomaly import pic_id_anomaly
from single_point import pic_singal_point
reload(sys)
sys.setdefaultencoding('utf8')


def risk_feature_analysis(path, custer="jiufu"):
    """
    this function used to integrate all pictures
    :param path:
    :param active_id:
    :param custer:
    :return:
    """
    img_path = "{0}/risk_features_{1}".format(Data_path, custer)
    print("image path {0}".format(img_path))
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    df = read_multi_csv(path=path)
    df = df.sort_values(by="timestamp")
    df = df.loc[(df.mobile.notnull()) & (df.ckid.notnull()) & (df.ip.notnull()) & (df.city.notnull())]
    # --- ID异常
    pic_id_anomaly(df=df, img_path=img_path)
    # --- 设备行为
    pic_device_behavior(df=df, img_path=img_path)
    # # ---- 单点特征
    pic_singal_point(df=df, img_path=img_path)


if __name__ == '__main__':
    csv_dir = "/Users/chaoxu/code/local-spark/Data/jiufu/risk_feature_csv"
    risk_feature_analysis(path=csv_dir, custer="jiufu_id_v3")
