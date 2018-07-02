# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2018/2/6
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import division, print_function
import keras.backend as K
import numpy as np
from itertools import product


def w_categorical_crossentropy(y_true, y_pred, weights):
    nb_cl = len(weights)
    final_mask = K.zeros_like(y_pred[:, 0])
    y_pred_max = K.max(y_pred, axis=1)
    y_pred_max = K.expand_dims(y_pred_max, 1)
    y_pred_max_mat = K.equal(y_pred, y_pred_max)
    for c_p, c_t in product(range(nb_cl), range(nb_cl)):
        final_mask += (K.cast(weights[c_t, c_p], K.floatx()) *
                       K.cast(y_pred_max_mat[:, c_p], K.floatx()) *
                       K.cast(y_true[:, c_t], K.floatx())
                       )
    return final_mask

if __name__ == '__main__':
    w_array = np.ones((2, 2))
    w_array[1, 0] = 1.2
    w_array[0, 1] = 1.2
    a = K.variable(np.array([1, 0, 0]))
    b = K.variable(np.array([1, 1, 1]))
    s = K.eval(w_categorical_crossentropy(a, b, w_array))
    print(s)



