# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/13
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from sklearn.cross_validation import train_test_split
import pandas as pd
from sklearn.utils import shuffle
import numpy as np


def splitData(df, ratio):
    """
    this file used to spite data avoid data balance problem
    """
    positiveData = df[df["label"] == 1].copy(deep=True)
    _positiveTrain, _positiveTest = train_test_split(positiveData, train_size=ratio, random_state=42)
    negativeData = df[df["label"] == 0].copy(deep=True)
    _negativeTrain, _negativeTest = train_test_split(negativeData, train_size=ratio, random_state=42)
    train_set = pd.concat([_positiveTrain, _negativeTrain])
    test_set = pd.concat([_positiveTest, _negativeTest])
    train_set_sh = shuffle(train_set,random_state=42)
    test_set_sh = shuffle(test_set,random_state=42)
    return train_set_sh, test_set_sh


def split_np(data, ratio):
    """
    this file used to split data from numpy data
    y in the last index
    :param data:
    :param ratio:
    :return:
    """
    pos_data_index = np.where(data[:, -1] == 1)
    neg_data_index = np.where(data[:, -1] == 0)
    pos_data = data[pos_data_index]
    neg_data = data[neg_data_index]
    _positiveTrain, _positiveTest = train_test_split(pos_data, train_size=ratio, random_state=42)
    _negativeTrain, _negativeTest = train_test_split(neg_data, train_size=ratio, random_state=42)
    train_set = np.concatenate([_positiveTrain, _negativeTrain], axis=0)
    train_set_sh = shuffle(train_set)
    test_set = np.concatenate([_positiveTest, _negativeTest], axis=0)
    test_set_sh = shuffle(test_set)
    return train_set_sh, test_set_sh
