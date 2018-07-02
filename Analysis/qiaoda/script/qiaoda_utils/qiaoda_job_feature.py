# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/26
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
主要是准备从乔达excel数据中抽取对应的特征
"""
from __future__ import division, print_function
import string
import re
import numpy as np
import pandas as pd

from sklearn.feature_extraction import stop_words

from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

import datetime
import re
import jieba
import jieba.analyse

education_dict = {
    "bachelor": "本科",
    "master": "硕士",
    "doctor": "博士",
    "mba&emba": "工商管理硕士",
    'mba': "工商管理硕士",
    'emba': "高级工商管理硕士"
}


def convert_edu(x):
    if x in education_dict:
        return education_dict[x]
    else:
        return x


def split_career(text):
    try:
        a = text.split(",")
        a += (3 - len(a)) * [u'未知']
        return a
    except:
        return [u"未知", u"未知", u"未知"]


rp = re.compile(ur'([0-9]+[k,w,K,W]?[\u4e00-\u9fff]?)')


def extract_pay(x):
    return rp.findall(x)[:2]


def get_pay_range(x):
    x_len = len(x)
    pay = "-".join(x)
    if x_len > 0:
        if u'元' in pay:
            low_pay = int(x[0].replace(u'元', u""))
        elif u'万' in pay:
            low_pay = int(x[0].replace(u'万', u"")) * 10000 / 12
        elif u'k' in pay:
            low_pay = int(x[0].replace(u'k', u"")) * 1000
        elif u'w' in pay:
            low_pay = int(x[0].replace(u'w', u"")) * 10000 / 12
        else:
            low_pay = int(x[0])
    else:
        low_pay = None

    if x_len > 1:
        if u'元' in pay:
            high_pay = int(x[1].replace(u'元', u""))
        elif u'万' in pay:
            high_pay = int(x[1].replace(u'万', u"")) * 10000 / 12
        elif u'k' in pay:
            high_pay = int(x[1].replace(u'k', u"")) * 1000
        elif u'w' in pay:
            high_pay = int(x[1].replace(u'w', u"")) * 10000 / 12
        else:
            high_pay = int(x[1])
    else:
        high_pay = None
    return (low_pay, high_pay)


def prepare_qiaoda_feture(fname, save_path):
    df = pd.read_excel(fname)
    # IMEI可能为空， 以及去掉\t和空格
    df = df.loc[df.IMEI.notnull()]
    df['IMEI'] = df['IMEI'].apply(lambda x: x.replace("\t", "")).apply(lambda x: x.replace(" ", ""))
    feature_cols = df.columns.difference(['IMEI'])
    print("feature columns of qiaoda is {0}".format(feature_cols))
    feature_obj_cols = df.select_dtypes(include=[np.object_]).columns.difference(['IMEI'])
    print("object feature columns is {1}".format(feature_obj_cols))
    # 错误编码数据填入 u'未知', 比如最高学位里面存在时间类型数据
    df[feature_obj_cols] = df[feature_obj_cols].applymap(format_check)
    # 最高学历转化
    # df[u'最高学历'] = df[u'最高学历'].apply(lambda x: convert_edu(x)).apply(lambda x: jieba.analyse.extract_tags(x))
    df[u'最高学历'] = df[u'最高学历'].apply(lambda x: convert_edu(x))
    # 所在地topic获取
    # df[u'所在地'] = df[u'所在地'].apply(lambda x: jieba.analyse.extract_tags(x))
    # 行业划分
    df[u'行业1'], df[u'行业2'], df[u'行业3'] = zip(*df[u'行业'].apply(lambda x: split_career(x)))
    # 薪资处理
    df[u'薪资'] = df[u'薪资'].apply(lambda x: extract_pay(x))
    df[u'薪资下限'], df[u'薪资上限'] = zip(*df[u'薪资'].apply(lambda x: get_pay_range(x)))
    df[[u'薪资下限', u'薪资上限']] = df[[u'薪资下限', u'薪资上限']].apply(np.sort, axis=1)
    df[u'薪资下限'] = df[u'薪资下限'].fillna(df[u'薪资下限'].mean())
    df[u'薪资上限'] = df[u'薪资上限'].fillna(df[u'薪资上限'].mean())
    df = df.drop([u'薪资', u'行业'], axis=1)
    # 对Unicode编码的column进行关键词抽取
    feature_obj_cols = df.select_dtypes(include=[np.object_]).columns.difference(['IMEI'])
    print("new object feature columns is {1}".format(feature_obj_cols))
    df[feature_obj_cols] = df[feature_obj_cols].applymap(jieba.analyse.extract_tags)

