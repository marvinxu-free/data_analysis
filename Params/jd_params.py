# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/1/19
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from collections import OrderedDict
from path_params import Data_path
import re
import numpy as np
data_path = "{0}/JData".format(Data_path)
holiday_data = "{0}/holiday.json".format(data_path)


train_login = "{0}/JData/t_login.csv".format(Data_path)
train_login_test = "{0}/JData/t_login_test.csv".format(Data_path)
train_trade = "{0}/JData/t_trade.csv".format(Data_path)
train_trade_test = "{0}/JData/t_trade_test.csv".format(Data_path)

one_hot_pkl = "{0}/JData/one_hot.pickle".format(Data_path)
feature_hash_pkl = "{0}/JData/feature_hash.pickle".format(Data_path)
train_data = "{0}/JData/train_features.npy".format(Data_path)
test_data = "{0}/JData/test_features.npy".format(Data_path)
img_path = "{0}/img_JData/".format(Data_path)

v2_one_hot_pkl = "{0}/JData/v2_one_hot.pickle".format(Data_path)
v2_feature_hash_pkl = "{0}/JData/v2_feature_hash.pickle".format(Data_path)
v2_train_data = "{0}/JData/train_features.csv".format(Data_path)
v2_test_data = "{0}/JData/test_features.csv".format(Data_path)
v2_img_path = "{0}/v2_img_JData/".format(Data_path)

v3_one_hot_pkl = "{0}/JData/v3_one_hot.pickle".format(Data_path)
v3_feature_hash_pkl = "{0}/JData/v3_feature_hash.pickle".format(Data_path)
v3_train_data = "{0}/JData/v3_train_features.npy".format(Data_path)
v3_test_data = "{0}/JData/v3_test_features.npy".format(Data_path)
v3_img_path = "{0}/v3_img_JData/".format(Data_path)

lstm_one_hot_pkl = "{0}/JData/lstm_one_hot.pickle".format(Data_path)
lstm_feature_hash_pkl = "{0}/JData/lstm_feature_hash.pickle".format(Data_path)
lstm_feature_scale_pkl = "{0}/JData/lstm_feature_scale.pickle".format(Data_path)
lstm_train_data = "{0}/JData/lstm_train_features.csv".format(Data_path)
lstm_test_data = "{0}/JData/lstm_test_features.csv".format(Data_path)
lstm_img_path = "{0}/lstm_img_JData".format(Data_path)
lstm_hdf5_file = "{0}/lstm_{1}_best.hdf5".format(data_path, "{0}")
lstm_model_file = "{0}/lstm_{1}_model.json".format(data_path, "{0}")

one_hot_dict = OrderedDict()
one_hot_dict['type'] = 4
one_hot_dict['weekth'] = 6
one_hot_dict['dayofweek'] = 8
one_hot_dict['day'] = 32
one_hot_dict['hour'] = 25

feature_hasher_dict = OrderedDict()
feature_hasher_dict['id'] = 32
feature_hasher_dict['device'] = 12
feature_hasher_dict['log_from'] = 3
feature_hasher_dict['ip'] = 13
feature_hasher_dict['city'] = 7
feature_hasher_dict['result'] = 3

new_ = re.compile('^new_.*')
new_match = np.vectorize(lambda x: bool(new_.match(x)))

label_ = re.compile('^label.*')
label_match = np.vectorize(lambda x: bool(label_.match(x)))
