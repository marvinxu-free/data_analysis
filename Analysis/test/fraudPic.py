# -*- coding: utf-8 -*-
# Project: xuchao_ml
# Author: chaoxu create this file
# Time: 2017/7/31
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function,division
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from Analysis.Pic.maxent_style import *

from sklearn.datasets import make_classification
from sklearn.decomposition import PCA
import pandas as pd

from imblearn.over_sampling import SMOTE
from Analysis.Pic.maxent_style import maxent_style,remove_palette
from sklearn.preprocessing import StandardScaler


def plot_resampling(ax, X, y, title):
    c0 = ax.scatter(X[y == 0, 0], X[y == 0, 1], label="Class #0",alpha=0.5)
    c1 = ax.scatter(X[y == 1, 0], X[y == 1, 1], label="Class #1",alpha=0.5)
    ax.set_title(title)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.spines['left'].set_position(('outward', 10))
    ax.spines['bottom'].set_position(('outward', 10))
    ax.set_xlim([-6, 8])
    ax.set_ylim([-6, 6])
    return c0, c1

data = pd.read_csv("../Data/creditcard.csv")
data['normAmount'] = StandardScaler().fit_transform(data['Amount'].values.reshape(-1, 1))
data = data.drop(['Time','Amount'],axis=1)
X = data.ix[:, data.columns != 'Class']
y = data['Class']
print(X.head())
print(y.head())

# Instanciate a PCA object for the sake of easy visualisation
pca = PCA(n_components=2)
# Fit and transform x to visualise inside a 2D feature space
X_vis = pca.fit_transform(X)

# Apply regular SMOTE
kind = ['regular', 'borderline1', 'borderline2', 'svm']
sm = [SMOTE(kind=k) for k in kind]
X_resampled = []
y_resampled = []
X_res_vis = []
for method in sm:
    X_res, y_res = method.fit_sample(X, y)
    X_resampled.append(X_res)
    y_resampled.append(y_res)
    X_res_vis.append(pca.transform(X_res))


@maxent_style
@remove_palette
def picSMOTE():
    f, ((ax1, ax2), (ax3, ax4), (ax5, ax6)) = plt.subplots(3, 2)
    # Remove axis for second plot
    ax2.axis('off')
    ax_res = [ax3, ax4, ax5, ax6]
    c0, c1 = plot_resampling(ax1, X_vis, y, 'Original set')
    for i in range(len(kind)):
        plot_resampling(ax_res[i], X_res_vis[i], y_resampled[i],
                    'SMOTE {}'.format(kind[i]))

    ax2.legend((c0, c1), ('Class #0', 'Class #1'), loc='center',
           ncol=1, labelspacing=0.)
    plt.tight_layout()
    plt.show(True)

picSMOTE()