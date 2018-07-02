# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/8/23
# Company : Maxent
# Email: chao.xu@maxent-inc.com
import re
value_match = re.compile('^.*value$')
anomaly_match = re.compile('^.*anomaly$')

features = {"value":1, "anomaly":2}
b = {x: y for (x, y) in features.items() if bool(value_match.match(x)) or bool(anomaly_match.match(x))}
# b = {}
# for (x, y) in features.items():
#     value_flg = value_match.match(x)
#     anomaly_flg = anomaly_match.match(x)
#     if value_flg or anomaly_flg:
#         print "pass"
#         b[x] = y

print b


