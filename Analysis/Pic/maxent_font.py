# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/8/17
# Company : Maxent
# Email: chao.xu@maxent-inc.com

import matplotlib.pyplot as plt

def xtick_font(ax=None,font_size=None,rotation=90):
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(font_size)
        tick.label.set_rotation(rotation)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(font_size)
        tick.label.set_rotation(rotation)
    return None


