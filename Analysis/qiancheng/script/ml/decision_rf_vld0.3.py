# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/11
# Company : Maxent
# Email: chao.xu@maxent-inc.com

"""
model:
    this file is used decision tree and rf to classfy fraud device
data:
    1. use 7:3
    2. change dataset from event to device
"""
from __future__ import print_function, division
from Utils.qiancheng.get_data import read_dev_data,df_main
import warnings
warnings.filterwarnings("ignore")


def main(file_name,postfix):
    df = read_dev_data(file_name)
    df_main(df=df, os='ios',postfix=postfix)
    df_main(df=df, os='android',postfix=postfix)

if __name__ == "__main__":
    # file_ = "/Users/chaoxu/code/local-spark/Data/qiancheng_data/qiancheng_sample_new_merge_0.09/data.csv"
    file_ = "/Users/chaoxu/code/local-spark/Data/qiancheng_dev/qiancheng_sample_new_merge_0.09/data.csv"
    main(file_,postfix=0.09)
    file_1 = "/Users/chaoxu/code/local-spark/Data/qiancheng_dev/qiancheng_sample_new_merge_0.05/data.csv"
    main(file_1,postfix=0.05)
