# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/13
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from __future__ import print_function, division

import xgboost as xgb
from Utils.get_oop import get_oof
from Utils.splitData import splitData
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
from sklearn.utils import shuffle

from Params.xgb_cv_params import xgb_base_params

SEED = 1234  # for reproducibility

def stack_algorithm(df):
    df_train, df_test = splitData(df, 0.7)
    print(df_train.shape, df_test.shape)
    df_train = shuffle(df_train)
    df_test = shuffle(df_test)
    X_train = df_train.ix[:, df_train.columns != 'label'].drop(['maxent_id'], axis=1)
    X_test = df_test.ix[:, df_test.columns != 'label'].drop(['maxent_id'], axis=1)
    y_train = df_train.ix[:, df_train.columns == 'label']
    y_test = df_test.ix[:, df_test.columns == 'label']
    print("Number transactions ios train dataset: ", X_train.shape[0])
    print("Number transactions ios test dataset: ", X_test.shape[0])
    X_train_cv = X_train
    y_train_cv = y_train['label']

    scale_ratio = df_train.loc[df_train.label == 1].shape[0] / df_train.loc[df_train.label == 0].shape[0]
    # rf = SklearnHelper(clf=RandomForestClassifier, seed=SEED, params={})
    # et = SklearnHelper(clf=tree.DecisionTreeClassifier, seed=SEED, params={})
    # ada = SklearnHelper(clf=AdaBoostClassifier, seed=SEED, params=ada_params)
    # gb = SklearnHelper(clf=GradientBoostingClassifier, seed=SEED, params=gb_params)
    # svc = SklearnHelper(clf=SVC, seed=SEED, params=svc_params)
    xgb_base_params['scale_pos_weight'] = scale_ratio

    #level1 to generate new train and test data for next level
    # et_oof_train, et_oof_test = get_oof(et, X_train.values, y_train.values, X_test.values,y_test.values)  # Extra Trees
    # rf_oof_train, rf_oof_test = get_oof(rf, X_train.values, y_train.values, X_test.values,y_test.values)  # Random Forest
    # # ada_oof_train, ada_oof_test = get_oof(ada, X_train, y_train, X_test,y_test)  # AdaBoost
    # # gb_oof_train, gb_oof_test = get_oof(gb,X_train, y_train, X_test) # Gradient Boost
    # # svc_oof_train, svc_oof_test = get_oof(svc,X_train, y_train, X_test) # Support Vector Classifier
    # print("level1 Training is complete")
    #
    # # feature importance
    # rf_feature = rf.feature_importances(X_train.values, y_train.values)
    # et_feature = et.feature_importances(X_train.values, y_train.values)
    # # ada_feature = ada.feature_importances(X_train, y_train)
    # # gb_feature = gb.feature_importances(X_train,y_train)
    #
    # base_predictions_train = pd.DataFrame({'RandomForest': rf_oof_train.ravel(),
    #                                        'Tree': et_oof_train.ravel(),
    #                                        # 'AdaBoost': ada_oof_train.ravel(),
    #                                        })
    # print(base_predictions_train.head(3))
    #
    # ##GridCv for xgboost for level 2
    # X_stack_train = np.concatenate((et_oof_train, rf_oof_train), axis=1)
    # X_stack_test = np.concatenate((et_oof_test, rf_oof_test), axis=1)
    # y_train_cv = y_train['label']
    # _params, _clf = gridCV(clf=xgb.XGBClassifier, X=X_stack_train, y=y_train_cv,
    #                        base_params=xgb_base_params, all_test_params=xgb_test_params, cv_parmas=cv_params)
    # clf = xgb.XGBClassifier(**_params)
    # clf_0 = clf.fit(X_stack_train, y_train)
    #
    # y_pred = clf_0.predict(X_stack_test)
    # print(classification_report(y_test, y_pred, target_names=['0', '1']))


    # _params, _clf = gridCV(clf=xgb.XGBClassifier, X=X_train, y=y_train_cv,
    #                        base_params=xgb_base_params, all_test_params=xgb_test_params, cv_parmas=cv_params)
    # clf = xgb.XGBClassifier(**_params)
    clf = xgb.XGBClassifier()
    # clf_0 = clf.fit(X_train, y_train)
    #
    # y_pred = clf_0.predict(X_test)
    # print(classification_report(y_test, y_pred, target_names=['0', '1']))

    clf_1 = RandomForestClassifier()
    # clf_0_1 = clf_1.fit(X_train, y_train)

    # y_pred_1 = clf_0_1.predict(X_test)
    # print(classification_report(y_test, y_pred_1, target_names=['0', '1']))

    clf_2 = tree.DecisionTreeClassifier()
    # clf_0_2 = clf_2.fit(X_train, y_train)
    #
    # y_pred_2 = clf_0_2.predict(X_test)
    # print(classification_report(y_test, y_pred_2, target_names=['0', '1']))

    et_oof_train_xgb, et_oof_test_xgb = get_oof(clf, X_train.values, y_train.values, X_test.values,y_test.values)  # Extra Trees
    et_oof_train_rf, et_oof_test_rf = get_oof(clf_1, X_train.values, y_train.values, X_test.values,y_test.values)  # Extra Trees
    et_oof_train_tree, et_oof_test_tree = get_oof(clf_2, X_train.values, y_train.values, X_test.values,y_test.values)  # Extra Trees
