from xgboost import XGBClassifier
import xgboost as xgb
from do_evaluation import evaluate_result
from sklearn.model_selection import GridSearchCV
import numpy as np
from prec_rec import prec_and_recall
from do_evaluation import *
from Utils.common.gridCV import gridCV
from Params.xgb_cv_params import xgb_base_params,xgb_test_params
from Params.cv_params import cv_params

def train_xgboost(train_set, cols,model_name = "appweb_xgb,model"):
    x = train_set[cols]
    y = train_set["label"]
    #clf = XGBClassifier(base_score=0.5, colsample_bylevel=0.5, colsample_bytree=0.8,
    #                    max_depth=3, n_estimators=200, objective='binary:logistic', seed=101,
    #                    gamma=0, learning_rate=0.1)
    # clf = XGBClassifier(base_score=0.5, booster='gbtree',
    #                     max_depth=3, objective='binary:logistic', random_state=101,
    #                     gamma=0)
    # grid = GridSearchCV(clf, param_grid={"colsample_bylevel": [0.5, 0.8, 1.0], "colsample_bytree": [0.8, 1.0],
    #                               "n_estimators": [100, 200], "learning_rate": [0.01, 0.1]})
    #clf.fit(x, y)
    xgb_base_params['missing'] = 0.0
    # y_train = train_set.ix[:, train_set.columns == 'label']
    # sum_pos = y_train.loc[y_train.label == 1].shape[0]
    # sum_neg = y_train.loc[y_train.label == 0].shape[0]
    # scale_ratio = sum_pos / sum_neg
    # xgb_base_params['scale_pos_weight'] = scale_ratio

    y_train_cv = y
    _params, clf_0 = gridCV(clf=xgb.XGBClassifier, X=x, y=y_train_cv,
                            base_params=xgb_base_params, all_test_params=xgb_test_params, cv_parmas=cv_params)
    # params = {"max_depth": 3, 'objective': 'binary:logistic', 'eta': 0.1, 'gamma': 0, 'colsample_bytree': 0.8,
    #           'colsample_bylevel': 0.5, 'base_score': 0.5, 'random_state': 101}
    print cols
    dtrain = xgb.DMatrix(train_set[cols].values.tolist(), label=train_set["label"].values.tolist())
    bst = xgb.train(params=_params, dtrain=dtrain, num_boost_round=200)
    bst.save_model(model_name)
    print "*****************************save model***************************"
    print bst.predict(dtrain, output_margin=True)[:10]
    print bst.feature_names
    # print(clf)
    # grid.fit(x, y)
    # print ("Best: %f using %s" % (grid.best_score_, grid.best_params_))
    return bst


def test_xgboost(clf, test_set, cols):
    x = xgb.DMatrix(test_set[cols].values.tolist())
    y = test_set["label"]
    #y_prob = clf.predict_proba(x)[:, 1]
    y_prob = clf.predict(x)
    alpha_list = []
    prec_list = []
    rec_list = []
    fscore = []
    tpr = []
    fpr = []
    # evaluate precision and recall  with different alpha
    alpha_list, prec_list, rec_list, fscore, tpr, fpr = evaluate_result(y_prob, y.values)
    # evaluate precision and recall with different ts-diff(alpha=0.9)
    test_set["score"] = y_prob
    time_series = []
    prec_series = []
    rec_series = []
    # prec_series, rec_series, time_series = prc_by_time(test_set)
    return alpha_list, prec_list, rec_list, fscore, tpr, fpr, time_series, prec_series, rec_series


def do_xgboost(train_data, test_data, cols,model_name):
    train_set = train_data.copy(deep=True)
    test_set = test_data.copy(deep=True)
    clf = train_xgboost(train_set, cols,model_name=model_name)
    alpha_list, prec, rec, fscore, tpr, fpr, time_series, prec_series, rec_series = test_xgboost(clf, test_set, cols)
    # plot_prc_by_time(time_series, prec_series, rec_series)
    return alpha_list, prec, rec, fscore, tpr, fpr, time_series, prec_series, rec_series
