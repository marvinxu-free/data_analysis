# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/8/15
# Company : Maxent
# Email: chao.xu@maxent-inc.com
import re
import numpy as np

ipSeg_a = re.compile('^ipSeg24.*anomaly$')
ipSeg_amatch = np.vectorize(lambda x:bool(ipSeg_a.match(x)))

ipSeg_v= re.compile('^ipSeg24.*value$')
ipSeg_vmatch = np.vectorize(lambda x:bool(ipSeg_v.match(x)))

ipSeg_al = re.compile('^ipSeg24.*anomaly.log$')
ipSeg_almatch = np.vectorize(lambda x:bool(ipSeg_al.match(x)))

ipGeo_a = re.compile('^ipGeo.*anomaly$')
ipGeo_amatch = np.vectorize(lambda x:bool(ipGeo_a.match(x)))

ipGeo_v = re.compile('^ipGeo.*value$')
ipGeo_vmatch = np.vectorize(lambda x:bool(ipGeo_v.match(x)))
ipGeo_al = re.compile('^ipGeo.*anomaly.log$')
ipGeo_almatch = np.vectorize(lambda x:bool(ipGeo_al.match(x)))

anormaly = re.compile('.*anomaly$')
anormaly_match = np.vectorize(lambda x:bool(anormaly.match(x)))

anormaly_l = re.compile('.*anomaly.log$')
anormaly_l_match = np.vectorize(lambda x:bool(anormaly_l.match(x)))

value = re.compile('.*.value$')
value_match = np.vectorize(lambda x:bool(value.match(x)))

maxentID_a = re.compile('^maxentID.*anomaly$')
maxenID_amatch = np.vectorize(lambda x:bool(maxentID_a.match(x)))
maxentID_v = re.compile('^maxentID.*value$')
maxenID_vmatch = np.vectorize(lambda x:bool(maxentID_v.match(x)))
maxentID_al = re.compile('^maxentID.*anomaly.log$')
maxenID_almatch = np.vectorize(lambda x:bool(maxentID_al.match(x)))


did_a = re.compile('^did.*anomaly$')
did_amatch = np.vectorize(lambda x:bool(did_a.match(x)))
did_v = re.compile('^did.*value$')
did_vmatch = np.vectorize(lambda x:bool(did_v.match(x)))
did_al = re.compile('^did.*anomaly.log$')
did_almatch = np.vectorize(lambda x:bool(did_al.match(x)))

fraud = re.compile("isSimulator.*anomaly | shopID.*.anomaly | refereeID.*.anomaly")
fraud_match = np.vectorize(lambda x:bool(fraud.match(x)))


