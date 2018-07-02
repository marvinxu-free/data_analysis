from sklearn.ensemble import GradientBoostingClassifier
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from do_evaluation import *
from prec_rec import prec_and_recall
import sys
from sklearn.metrics import roc_curve
from sklearn.pipeline import Pipeline
import pickle
import numpy as np


def get_feat_importances(df, trees):
    feature_importances = pd.DataFrame(zip(df.columns, trees.feature_importances_))
    feature_importances.columns = ["features", "value"]
    feature_importances_position = feature_importances[feature_importances.value > 0]
    print feature_importances_position.sort_values("value", ascending=False)


def train_gbdt(train_set, cols):
    # x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3)
    x_train = train_set[cols]
    y_train = train_set["label"]
    # pickle.dumps(open("y_test", "wb"))
    x_train, x_train_lr, y_train, y_train_lr = train_test_split(x_train, y_train, test_size=0.3, random_state=30)
    # grd = GradientBoostingClassifier(n_estimators=140, max_depth=7, random_state=27)
    # pipe = Pipeline([("model1", GradientBoostingClassifier()), ("model2", OneHotEncoder()), ("model3", LogisticRegression(fit_intercept=True,
    #                     class_weight='balanced', random_state=30, max_iter=10000))])
    # param_grid = {"model1__n_estimators": [5, 10, 30, 50, 80, 100, 120, 130, 140, 145, 150, 180, 200],
    #               "model1__max_depth": [3, 5, 7, 10, 15]}
    # n_estimators = int(sys.argv[2])
    # max_depth = int(sys.argv[3])
    # c = float(sys.argv[4])
    # grd = GradientBoostingClassifier(random_state=27, n_estimators=80, max_depth=3)  # app_web hyperparameters
    grd = GradientBoostingClassifier(random_state=27, n_estimators=150, max_depth=3)  # app_app hyperparameters
    # grd = GradientBoostingClassifier(random_state=27, n_estimators=200, max_depth=3)
    # grd = GradientBoostingClassifier(random_state=27, n_estimators=n_estimators, max_depth=max_depth)
    grd_enc = OneHotEncoder()
    # grd_lm = LogisticRegression(fit_intercept=True, C=10.0, class_weight='balanced', random_state=30, max_iter=10000)  # app_web hyperparameters
    grd_lm = LogisticRegression(fit_intercept=True, C=0.001, class_weight='balanced', random_state=30, max_iter=10000)  #app_app hyperparameters
    # grid = GridSearchCV(pipe, param_grid=param_grid, n_jobs=1)
    grd.fit(x_train, y_train)
    # grid.fit(x_train, y_train)
    # print("Best: %f using %s" % (grid.best_score_, grid.best_params_))
    # print get_feat_importances(x_train, grd)
    grd_enc.fit(grd.apply(x_train)[:, :, 0])
    grd_lm.fit(grd_enc.transform(grd.apply(x_train_lr)[:, :, 0]), y_train_lr)
    return grd, grd_enc, grd_lm


def test_gbdt(grd, grd_enc, grd_lm, test_set, cols):
    x_test = test_set[cols]
    y_test = test_set["label"]
    alpha_list = []
    prec_list = []
    rec_list = []
    fscore = []
    tpr = []
    fpr = []
    y_prob = grd_lm.predict_proba(grd_enc.transform(grd.apply(x_test)[:, :, 0]))[:, 1]
    # fpr_grd_lm, tpr_grd_lm, _ = roc_curve(y_test, y_prob)
    alpha_list, prec_list, rec_list, fscore, tpr, fpr = evaluate_result(y_prob, y_test.values)
    test_set["score"] = y_prob
    prec_series, rec_series, time_series = prc_by_time(test_set)
    # print "precision:", prec[90], " recall:", rec[90], " fscore:", fscore[90]
    return alpha_list, prec_list, rec_list, fscore, tpr, fpr, time_series, prec_series, rec_series

def do_gbdt(train_data, test_data, cols):
    """
    This function is to train and test gbdt model

    Returns:
        model evaluation: alpha, prec, rec, fscore, tpr, fpr
    """

    # pickle.dump(test_set, open("test_set", "wb"))
    train_set = train_data.copy(deep=True)
    test_set = test_data.copy(deep=True)
    grd, grd_enc, grd_lm = train_gbdt(train_set, cols)
    alpha_list, prec, rec, fscore, tpr, fpr, time_series, prec_series, rec_series = test_gbdt(grd, grd_enc, grd_lm, test_set, cols)
    # plot_prc_by_time(time_series, prec_series, rec_series)
    # visualizeRoc(fpr, tpr, "new model")
    return alpha_list, prec, rec, fscore, tpr, fpr, time_series, prec_series, rec_series