# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/9
# Company : Maxent
# Email: chao.xu@maxent-inc.com
from __future__ import print_function, division
from keras.models import Sequential
from keras.layers import Dense, LSTM, Activation, Embedding
from sklearn.metrics import classification_report
from keras.callbacks import ModelCheckpoint
from Utils.common.keras_udf_metric import pos_recall, pos_precision, pos_fmeasure
from Utils.common.keras_udf_loss import bin_categorical_crossentropy, wrapped_partial
from sklearn.utils import class_weight
from Pic.pic_keras import pic_history
import numpy as np
from Params.jd_params import *
import json
import random as rn
np.random.seed(42)
rn.seed(12345)


def lstm_model(X_train, X_test, y_train, y_test, post_fix='1v'):
    model = Sequential()
    model.add(
        LSTM(128,
             input_shape=(X_train.shape[1], X_train.shape[2]),
             dropout=0.4,
             recurrent_dropout=0.05)
    )
    # model.add(LSTM(128, dropout=0.2, recurrent_dropout=0.2))
    # model.add(Dense(2))
    model.add(Dense(1,
                    activation='sigmoid',
                    # kernel_initializer=lecun_uniform(2),
                    # kernel_regularizer=regularizers.l2(0.01),
                    # use_bias=True,
                    # bias_initializer=lecun_uniform(5),
                    # bias_regularizer=regularizers.l1(0.01)
                    ))
    # model.add(Activation("sigmoid"))

    w_array = np.ones((2, 2))
    w_array[1, 0] = 1.5
    w_array[0, 1] = 1.5
    class_weights = class_weight.compute_class_weight('balanced', (0, 1), y_train.reshape(1, -1)[0])
    class_weights_ = {}
    for x, y in enumerate(class_weights):
        class_weights_[x] = y
    print(class_weights_)

    # udf_loss = partial(w_categorical_crossentropy, weights=w_array)

    udf_loss = wrapped_partial(bin_categorical_crossentropy, e1=1.2, e2=0.8)
    model_chk_hdf5 = lstm_hdf5_file.format(post_fix)
    model_file = lstm_model_file.format(post_fix)
    print(model_chk_hdf5)
    # checkpointer = ModelCheckpoint(filepath=model_chk_hdf5, monitor='val_pos_fmeasure',
    #                                verbose=1, save_best_only=True)

    model.compile(
        loss=udf_loss,
        # loss='binary_crossentropy',
        optimizer='adam',
        # sample_weight_mode="temporal",
        metrics=[pos_precision, pos_recall, pos_fmeasure]
    )

    # fit network
    history = model.fit(X_train, y_train,
                        epochs=20,
                        batch_size=10000,
                        # class_weight={0: 1., 1: 100.},
                        # class_weight=class_weights_,
                        validation_data=(X_test, y_test),
                        verbose=2,
                        # callbacks=[checkpointer],
                        shuffle=False
                        )
    y_pred_prob = model.predict(X_test)
    y_pred = np.round(y_pred_prob)
    print(classification_report(y_test, y_pred, target_names=['0', '1']))
    with open(model_file, 'w') as f:
        model_json_str = model.to_json()
        json.dump(model_json_str, f, indent=4, sort_keys=True)
    model.save_weights(model_chk_hdf5, overwrite=True)
    metric_rpt = [
        ('pos_precision', u'训练集正样本准确率'),
        ('pos_recall', u'训练集正样本召回率'),
        ('val_pos_precision', u'验证集正样本准确率'),
        ('val_pos_recall', u'验证集正样本召回率'),
        ('pos_fmeasure', u'训练集正样本F score(beta=0.1)'),
        ('val_pos_fmeasure', u'训练集正样本F score(beta=0.1)')
    ]
    pic_history(history=history.history, metrics_reports=metric_rpt, title=u'LSTM Metric Report',
                img_file="{0}/lstm_metrics.png".format(lstm_img_path))
