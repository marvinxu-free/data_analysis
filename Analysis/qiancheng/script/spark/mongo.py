# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/9/22
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pymongo import MongoClient
import sys
import datetime
fname = sys.argv[1]
client = MongoClient("10.11.2.51", 27017)
db = client["idservice"]
coll = db["event"]
time_list = []
maxent_id_list = []
import json
save_path = '/Users/chaoxu/code/local-spark/Data/qiancheng_mogo.txt'
def get_doc():
    doc_list = []
    try:
        with open(save_path) as f:
            for line in f.readlines():
                line_dict = json.loads(line)
                doc_list.append(line_dict)
    except Exception as e:
        with open(fname) as f:
            for line in f.readlines():
                maxent_id = line.strip()
                print "find maxent_id {0}".format(maxent_id)
                for doc in coll.find({"maxent_id": maxent_id}):
                    # doc.pop("_id")
                    # doc["save_time"] = doc["save_time"].strftime("%Y-%m-%d %T")
                    a = {}
                    a['maxent_id'] = doc['maxent_id']
                    a['timestamp'] = int(doc['timestamp']) / 1000.0
                    doc_list.append(a)
        with open(save_path,'w') as f:
            for line in doc_list:
                line_str = json.dumps(line) + '\n'
                f.write(line_str)
    return doc_list

if __name__ == '__main__':
    doc_list = get_doc()
    time_list = map(lambda x:x['timestamp'], doc_list)
    max_time = max(time_list)
    min_time = min(time_list)
    max_time_str = datetime.datetime.fromtimestamp(max_time).strftime("%Y-%m-%d %H:%M:%S")
    min_time_str = datetime.datetime.fromtimestamp(min_time).strftime("%Y-%m-%d %H:%M:%S")
    print "max date is {0}".format(max_time_str)
    print "min date is {0}".format(min_time_str)
    doc_need = filter(lambda x:x['timestamp'] > 1501516800 and x['timestamp'] < 1503158400, doc_list)
    maxent_id_appeared = set(map(lambda x:x['maxent_id'], doc_need))
    print "maxnet_id appeared in 0801-0820 num is {0}".format(len(maxent_id_appeared))
    save_need_path ='/Users/chaoxu/code/local-spark/Data/qiancheng_mogo_588.txt'
    with open(save_need_path,'w') as f:
        for line in doc_need:
            line_str = json.dumps(line) + '\n'
            f.write(line_str)
    time_need_list = map(lambda x:x['timestamp'],doc_need)
    need_time_max = max(time_need_list)
    need_time_min = min(time_need_list)
    print "max need time is {0}".format(need_time_max)
    print "min need time is {0}".format(need_time_min)


