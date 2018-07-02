# -*- coding: utf-8 -*-
# Project: maxent-ml
# Author: chaoxu create this file
# Time: 2017/8/30
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from contextlib import contextmanager
from multiprocessing import Pool
import fnmatch
import os
from itertools import chain
import glob
import pandas as pd


@contextmanager
def custom_open(filename):
    f = open(filename)
    try:
        yield f
    finally:
        f.close()


def read_iter(filename):
    with open(filename, "r") as f:
        return f.readlines()


def read_files(path, matcher="part-*"):
    """
    this function used muti threads to read files from path
    :param path:
    :param matcher:
    :return:new iter
    """
    files_ = fnmatch.filter(os.listdir(path), matcher)
    obs_files = map(lambda x: path+"/"+x, files_)
    pool = Pool(processes=8)
    content_list = pool.map(read_iter, obs_files)

    data = reduce(chain, content_list)
    return data


def read_multi_csv(path, head=True):
    """
    this function used to read multi csv files and concate to one dataframe
    in pandas
    :param path:
    :param head:
    :return:
    """
    csv_reg = path + "/*.csv"
    csv_files = glob.iglob(csv_reg)
    if head:
        df_list = (pd.read_csv(f, engine='c') for f in csv_files if is_file_empty(f))
    else:
        df_list = (pd.read_csv(f, header=None, engine='c') for f in csv_files)
    df = pd.concat(df_list, ignore_index=True)
    df = df.reset_index()
    return df


def is_file_empty(file_name):
    return True if os.path.isfile(file_name) and os.path.getsize(file_name) > 0 else False
