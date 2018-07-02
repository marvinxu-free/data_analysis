# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/8
# Company : Maxent
# Email: chao.xu@maxent-inc.com
import re

r_age = re.compile(ur"年龄段.*?([0-9]+-?[0-9]*)")
re_edu = re.compile(ur';(本科.*?|研究生.*?|博士.*?|专科.*?|高中.*?|小学.*?|初中.*?|中专.*?);')
re_eco = re.compile(ur'(\b有车\b|\b有房\b|\b较高\b)', re.UNICODE)
re_comsu = re.compile(ur'(\b购物\b|\b团购\b|\b支付\b)', re.UNICODE)
re_act = re.compile(ur'(借款|欠款|借贷|还款|欠款|欠贷|借|网贷)', re.UNICODE)
re_new_card = re.compile(ur'经常换卡', re.UNICODE)
re_app = re.compile(ur'小贷app总个数.*?([0-9,\|]+)', re.UNICODE)


def get_sex(x):
    if isinstance(x, unicode):
        if u"男" in x:
            return u'男'
        elif u"女" in x:
            return u"女"
        else:
            return u"未知"
    else:
        return u"未知"


def get_ages(x):
    try:
        ages =  r_age.findall(x)[0].split("-")
        ages_len = len(ages)
        if ages_len == 0:
            return [None, None]
        elif ages_len == 1:
            return [int(ages[0]), int(ages[0])]
        else:
            return [int(ages[0]), int(ages[1])]
    except Exception as e:
        return [None, None]


def get_edu(x):
    try:
        edu = re_edu.findall(x)[0]
        return edu
    except Exception as e:
        return u"未知"


def get_eco_level(x):
    try:
        s = re_eco.search(x)
        if x == u'未知':
            return 2
        elif s:
            return 1
        else:
            return 0
    except:
        return 0


def get_consume_feature(x):
    try:
        s = re_comsu.search(x)
        if x == u'未知':
            return 2
        elif s:
            return 1
        else:
            return 0
    except:
        return 0


def get_act_feature(x):
    try:
        s = re_act.search(x)
        if x == u'未知':
            return 2
        elif s:
            return 1
        else:
            return 0
    except:
        return 0


def get_credit(x):
    try:
        if x == u'未知':
            new_card = 2
        elif re_new_card.search(x):
            new_card = 1
        else:
            new_card = 0

        if x == u'未知':
            app = 2
        elif re_app.search(x):
            app = 1
        else:
            app = 0
        return (new_card, app)
    except:
        return (0, 0)



