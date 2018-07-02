# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to prepare feature from JD data
唯一的标注数据来源于交易数据。
1. merge登陆数据以及交易数据
2. 对merge的数据:
    a. 按照账户ID以及天来进行group
    b. 如果当前用户在当天存在异常的交易， 所有的登陆数据全部标为label=1. 以此来产生新的标注数据
3. 对特征的处理：
    a. 部分特征比如ip, city存在分类多， 训练数据中的数据不能完全覆盖全集的问题， 因此此处使用的是feature hash的方法转为不同的矢量进行表示
    b. 对于有限分类特征， 比如登陆来源， 登陆结果使用label encoder
    c. 以上分类数据编解码的时候， 都必须先转化为str类型
    d. 编码模型存储到本地， 方便后续继续使用转化test数据
    e. bool特征不处理,直接转为int
4. 以上, 为了避免特征归一化需要，使用RF作为机器学习算法
5. 当交易欺诈行为只和前一次的登陆行为有关， 类似markov过程。登陆行为作为输入序列， 交易风险作为状态输出；从输入到输出的隐含状态则是我们需要使用机器学习模型解决的。
"""
from __future__ import print_function, division
from Params.jd_params import *
import pandas as pd
from Pic.hist import pic_label
import argparse
import os
import errno
from feature_utils.create_new_col import create_new_col
from feature_utils.feature_encode import feature_encode


def prepare_features(f1, f2):
    """

    :param f1: file for login data
    :param f2: file for trade data
    :return:
    """
    if os.path.exists("debug.csv"):
        df_z = pd.read_csv('debug.csv')
    else:
        print(f1)
        print(f2)
        df_trade = pd.read_csv(f2)
        df_trade['time'] = pd.to_datetime(df_trade['time'], utc=True)
        df_login = pd.read_csv(f1)
        df_login[['is_scan', 'is_sec']] = df_login[['is_scan', 'is_sec']].astype(int)
        df_login = df_login.drop('timestamp', axis=1)
        df_login['time'] = pd.to_datetime(df_login['time'], utc=True)
        df_login = df_login.rename(columns={'time': 'login_time'})

        df = df_login.merge(df_trade, on='id', how='left')
        df = df.loc[df.login_time < df.time]
        df = df.sort_values(['login_time', 'time'], ascending=[True, True])
        df_trade = df.groupby('rowkey').last().reset_index()
        df = df_login.merge(df_trade[['login_time', 'is_risk', 'id']], on=['id', 'login_time'], how='left')
        df['is_risk'] = df['is_risk'].fillna(0)
        if not os.path.exists(img_path):
            try:
                os.makedirs(img_path)
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise
        pic_label(df=df, col='is_risk', title=u"原始登陆数据产生风险比例", file_path=img_path)
        df_z = create_new_col(df)
        df_z.to_csv('debug.csv', index=False)
    pic_label(df=df_z, col='label', title=u"关联登陆数据产生风险比例", file_path=img_path)
    feature_encode(df_z)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--prepare_mode', action='store', type=str, default='train', dest='prepare_mode',
                        help="prepare feature for train/test")
    FLAGS, _ = parser.parse_known_args()
    if FLAGS.prepare_mode == 'train':
        prepare_features(train_login, train_trade)
    else:
        prepare_features(train_login_test, train_trade_test)
