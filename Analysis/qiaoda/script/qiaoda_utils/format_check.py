# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com


def format_check(x):
    try:
        if isinstance(x, unicode) or isinstance(x, str):
            x_new = x.replace(" ", "").lower()
        else:
            x_new = u'未知'
        if x_new == "":
            return u'未知'
        else:
            return x_new
    except Exception as e:
        return u'未知'