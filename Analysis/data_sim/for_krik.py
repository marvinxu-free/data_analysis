# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/1
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import print_function, division
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from Pic.maxent_style import maxent_style


def df_nor():
    df1 = pd.DataFrame(np.random.randint(10, 200, 30), columns=[u'消费'],
                       index=range(30))
    df2 = pd.DataFrame(np.random.randint(140, 160, 10), columns=[u'消费'],
                       index=range(10))
    df = pd.concat([df2, df1], axis=0).reset_index(drop=True)
    return df


def df_anor():
    df = pd.DataFrame(np.random.randint(148, 152, 36), columns=[u'消费'],
                      index=range(36)).reset_index(drop=True)
    return df


def get_df(start, end, num, col):
    df = pd.DataFrame(np.random.randint(start, end, num), columns=[col],
                      index=range(num)).reset_index(drop=True)
    return df


@maxent_style
def fraud_consume(fname, dpi=600, palette=None):
    """
    生成欺诈展示图
    :return:
    """
    # df1 = pd.DataFrame(np.random.randint(100, 200, 100), columns=[u'消费'],
    #                    index=pd.date_range(start='20170101', periods=100, freq='D'))
    # df2 = pd.DataFrame(np.random.randint(150, 155, 50), columns=[u'消费'],
    #                    index=pd.date_range(start='20170411', periods=50, freq='D'))
    # df3 = pd.DataFrame(np.random.randint(100, 200, 100), columns=[u'消费'],
    #                    index=pd.date_range(start='20170531', periods=100, freq='D'))
    # df1_list = map(lambda x: df_nor(), range(5))
    # df1 = pd.concat(df1_list, axis=0)
    # df2_list = map(lambda x: df_anor(), range(1))
    # df2 = pd.concat(df2_list, axis=0)
    # df3_list = map(lambda x: df_nor(), range(6))
    # df3 = pd.concat(df3_list, axis=0)
    df1 = get_df(start=10, end=100, num=100, col=u'消费')
    df1.at[-1, u'消费'] = 10
    df2 = get_df(start=200, end=300, num=20, col=u'消费')
    df3 = get_df(start=10, end=100, num=100, col=u'消费')
    df2.at[0, u'消费'] = df1.iloc[-1].values[0]
    # df2.at[-1, u'消费'] = df3.iloc[0].values[0]

    df = pd.concat([df1, df2, df3], axis=0)
    df = df.reset_index(drop=True)
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    df.plot.line(ax=ax, logy=True)
    anor_index = [df1.shape[0], df1.shape[0] + df2.shape[0]]
    df.iloc[anor_index[0]:anor_index[1]].rename(columns={u'消费': u'异常消费'}).plot.line(ax=ax, c='r', logy=True)
    ax.set_title(u"Collective Anomalies")
    ax.set_ylabel(u"消费金额")
    ax.xaxis.set_ticks([])
    fig.subplots_adjust(bottom=0.2, top=0.95)
    fig.savefig(filename=fname, dpi=dpi, format='png')
    plt.show(block=False)


if __name__ == '__main__':
    fraud_consume("./1.png")
