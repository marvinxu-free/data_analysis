# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/22
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function, division
from Params.jd_params import *
import numpy as np
from Algorithm.jd_xgboost import jd_xgb, jd_xgb2, jd_xgb3


def train_model():
    # data = np.load(v3_train_data)
    # jd_xgb(data=data, postfix='2v')
    # jd_xgb2(missing=-0.1,postfix='2v')
    jd_xgb3(missing=-0.1, postfix='3v')


if __name__ == '__main__':
    train_model()
