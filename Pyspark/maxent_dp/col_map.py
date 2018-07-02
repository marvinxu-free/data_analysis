# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/5/16
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
本文件主要用于将原始数据的字段转换到Excel的列名
"""

common_map_cols = {
    u'操作系统': 'os',
    u'手机品牌': 'brand',
    u'手机型号': 'model',
    u'上网方式': 'ns',
    u'渠道号': 'pid',
    u'子渠道号': 'sub_pid',
    u'事件会话号': 'session_id',
    u'app应用版本': 'appversion',
    u'sdk版本': 'sdkversion',
    u'事件唯一标识符': 'event_id',
    u'用户名': 'userid',
    u'激活时的消息类型': '__tact',
    u'消息类型': 'typeclass',
    u'客户自定义内容': 'customMsg',
    u'是否root': 'is_cracked',
    u'猛犸提供的ID账户(与TID一致)': 'tid',
    u'时间戳': 'timestamp',
    u'设备ID': 'maxent_id',
    u'分辨率': 'resolution',
    u'运营商': 'cr',
    u'设备机型异常': 'ua_mismatch',
    u'代理检查异常': 'is_proxy',
    u'模拟器检测异常': 'is_simulator',
    u'设备破解': 'is_cracked',
    u'国家': 'country',
    u'国家代码': 'country_code',
    u'省': 'province',
    u'市': 'city',
    u'设备类型': 'device',
    u'campaign_id': 'campaign_id',
    u'UA信息': 'user_agent',
    u'Cookie ID': 'ckid',
}

dongfang_col_map = {
    'tick': 'tick',
    'type': 'type',
    'pid': 'pid',
    'sub_pid': 'sub_pid',
    "sub_pid_seller": '__seller_user_id',
    'tcpts':'tcpts',
    'window':'window',
    'timestamp':'timestamp',
    'maxentID':'maxent_id',
    'ipSeg24':'ipSeg24',
    'city':'city',
    'ipGeo_5m_value':'ipGeo.5m.value',
    'proxyIP_value':'proxyIP.value',
    'proxyIP_anomaly':'proxyIP.anomaly',
    'isSimulator_value':'uaMismatch.value',





}
