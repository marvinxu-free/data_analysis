# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/4/3
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used hadoop shell command for pyspark
"""
import subprocess


def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


def chk_dir_exist(hdfs_dir_path):
    cmd = ['hadoop', 'fs', '-test', '-d', hdfs_dir_path]
    code = run_cmd(cmd)
    if code == 1:
        print 'dir {0} not exist'.format(hdfs_dir_path)
        return False
    else:
        return True


def chk_file_exist(hdfs_file_path):
    cmd = ['hadoop', 'fs', '-test', '-e', hdfs_file_path]
    code = run_cmd(cmd)
    if code == 1:
        print 'file {0} not exist'.format(hdfs_file_path)
        return False
    else:
        return True
