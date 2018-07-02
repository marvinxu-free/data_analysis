# !/usr/bin/python
# coding:utf-8
# 实现从entropy server RPC读取数据
# import json
import avro.protocol as proto
import avro.ipc as ipc
import avro.io as avroio
import avro.schema as schema
import pandas as pd
from sqlalchemy import create_engine
PROTOCOL = proto.parse(open("entropy_server.json", "r").read())
client = ipc.HTTPTransceiver("10.11.1.17", 6522)
requestor = ipc.Requestor(PROTOCOL, client)


def ip_into_int(ip):
    # 先把 192.168.1.13 变成16进制的 c0.a8.01.0d ，再去了“.”后转成10进制的 3232235789 即可。
    # (((((192 * 256) + 168) * 256) + 1) * 256) + 13
    return reduce(lambda x, y: (x << 8) + y, map(int, ip.split('.')))


def is_internal_ip(ip):
    ip = ip_into_int(ip)
    net_a = ip_into_int('10.255.255.255') >> 24
    net_b = ip_into_int('172.31.255.255') >> 20
    net_c = ip_into_int('192.168.255.255') >> 16
    return ip >> 24 == net_a or ip >> 20 == net_b or ip >> 16 == net_c

# if __name__ == '__main__':


def test(method, msg):
    ips = msg["ip"]
    if ips.find(",") > 0:
        for ip in ips.split(","):
            if not is_internal_ip(ip):
                ip_ = ip
                msg["ip"] = ip_
                result = requestor.request(method, msg)
                break
    else:
        result = requestor.request(method, msg)
    return result


def main():
    print "========================================"
    # ua = u'Mozilla/5.0 (Linux; U; Android 6.0; zh-CN; PRO 6 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/40.0.2214.89 UCBrowser/11.3.8.909 UWS/2.10.2.5 Mobile Safari/537.36 UCBS/2.10.2.5 Nebula AlipayDefined(nt:NotReachable,ws:360|0|3.0) AliApp(AP/10.0.18.062203) AlipayClient/10.0.18.062203 Language/zh-Hans useStatusBar/true'
    ua = 'Mozilla/5.0 (Linux; U; Android 4.3; zh-cn; SM-W2014 Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
    msg = {'osVersion': 'ANDROID_PHONE',
          'ua': ua,
          'country': 'CHN',
            "ip": "203.93.127.254"}

    # method = "getUserAgentEntropy"
    method = "getIPEntropy"
    import json
    print json.dumps(test(method, msg))
    # ua = u'Mozilla/5.0 (Linux; Android 6.0; PRO 6 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/44.0.2403.146 Mobile Safari/537.36'
    ua = 'Mozilla/5.0 (iPhone 92; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 MQQBrowser/7.4.1 Mobile/14E304 Safari/8536.25 MttCustomUA/2 QBWebViewType/1 WKType/1'
    ua ="Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_2 like Mac OS X) AppleWebKit/603.2.4 (KHTML, like Gecko) Mobile/14F89 Nebula PSDType(1) AlipayDefined(nt:WIFI,ws:414|672|3.0) AliApp(AP/10.0.19.062202) AlipayClient/10.0.19.062202 Language/zh-Hans"
    ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12B436"
    # ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12B436"

    ua = "Mozilla/5.0 (Linux; Android 6.0.1; SM-G9208 Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36"
    ua = "Mozilla/5.0 (Linux; U; Android 5.0.2; zh-cn; SM-A7009 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/37.0.0.0 MQQBrowser/7.2 Mobile Safari/537.36"
    msg['ua'] = ua
    msg['osVersion'] = "ANDROID_PHONE"
    import json
    method = "getUserAgentEntropy"
    print json.dumps(test(method, msg))
    print requestor.request("getDirichletEntropy", {"type": "brand", "ttl": 64, "window": 75616, "wscale": 6, "name": "Samsung"})
    msg = {'osVersion': 'ANDROID_PHONE',
           'country': 'CHN',
           'ua': u'Mozilla/5.0 (Linux; Android 4.4.4; Che1-CL20 Build/Che1-CL20) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36',
           'ip': '112.97.56.174'}
    print requestor.request("getIPEntropy", msg)["ip"]
    # engine = create_engine("mysql+mysqlconnector://root:123456@ca-p02.mxnt.com:3306")
    # sql = """select * from entropy_server.idc_ip where inet_aton("198.162.1.1") between inet_aton(start) and inet_aton(end)"""
    # print pd.read_sql_query(sql, engine).shape[0]


if __name__ == "__main__":
    ip = '192.168.0.1'
    print ip, is_internal_ip(ip)
    ip = '10.2.0.1'
    print ip, is_internal_ip(ip)
    ip = '172.16.1.1'
    print ip, is_internal_ip(ip)
    main()
    ips = "192.168.1.1,10.10.0.1,116.228.150.254,182.92.190.178"
    if ips.find(",") > 0:
        for ip_ in ips.split(","):
            if not is_internal_ip(ip_):
                ip = ip_
                break
    else:
        ip = ips

    print ip
