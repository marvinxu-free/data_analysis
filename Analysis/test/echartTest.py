# -*- coding: utf-8 -*-
# Project: xuchao_ml
# Author: chaoxu create this file
# Time: 2017/7/24
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from pyecharts import Bar

bar = Bar("我的第一个图表", "这里是副标题")
bar.add("服装", ["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"], [5, 20, 36, 10, 75, 90])
bar.show_config()
bar.render()

