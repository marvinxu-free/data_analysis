# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/20
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
总数50057设备
经过相似度计算之后：47578
"""
from __future__ import division, print_function
import networkx as nx
from Utils.common.custerReadFile import custom_open
import json


def get_connect_devices(fname):
    G = nx.Graph()
    similar_device_num = 0
    with custom_open(fname) as f:
        for similar_json in f:
            doc = json.loads(similar_json)
            similar_data = doc.get("similarDevice")
            source_dev = doc['device']['device_id']
            if len(similar_data) > 0 :
                similar_edges = map(lambda x: (source_dev,x['device']['device_id']), similar_data)
                G.add_edges_from(similar_edges)
            else:
                G.add_node(source_dev)

    for i in nx.connected_components(G):
        print("sub connect graph is {0}".format(i))
        similar_device_num +=1
    print("similar devices num is {0}".format(similar_device_num))


if __name__ == '__main__':
    fname = "/Users/chaoxu/code/local-spark/Data/similary_device/result.json"
    get_connect_devices(fname=fname)
