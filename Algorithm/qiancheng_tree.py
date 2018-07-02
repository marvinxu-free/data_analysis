# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/10/18
# Company : Maxent
# Email: chao.xu@maxent-inc.com

from Utils.common.gridCV import gridCV
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.metrics import classification_report

from Params.cv_params import cv_params
from Params.rf_params import rf_base_params, rf_test_params
from Params.tree_params import tree_base_params, tree_test_params
from Utils.common.splitData import splitData
from Pic.confusion import plot_confusion_matrix_sns
from Pic.pic_tree import pic_feature_importance, pic_tree,pic_performence
from Pic.hist import pic_label
import errno
import os

img_save_path = "/Users/chaoxu/code/local-spark/Data/image_qiancheng_dev_{0}_{1}"

def qiancheng_tree_rf(df,os_sys='ios',postfix=0.09):
    print('run decision tree for {0}'.format(os_sys))
    df_train, df_test = splitData(df, 0.7)
    print(df_train.shape, df_test.shape)
    X_train = df_train.ix[:, df_train.columns != 'label']
    X_test = df_test.ix[:, df_test.columns != 'label']
    y_train = df_train.ix[:, df_train.columns == 'label']
    y_test = df_test.ix[:, df_test.columns == 'label']
    print("Number transactions {0} train dataset:".format(os_sys), X_train.shape)
    print("Number transactions {0} test dataset:".format(os_sys), X_test.shape)
    X_train_cv = X_train
    y_train_cv = y_train['label']

    # check and create file
    img_path = img_save_path.format(os_sys,postfix)
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise

    pic_label(df=df,col='label',title="{0} label ratio".format(os_sys),file_path=img_path)
    tree_params, clf_tree_0 = gridCV(clf=tree.DecisionTreeClassifier, X=X_train_cv, y=y_train_cv,
                                   base_params=tree_base_params, all_test_params=tree_test_params, cv_parmas=cv_params)
    print('decision tree')
    y_pred_tree = clf_tree_0.predict(X_test)
    print(classification_report(y_test, y_pred_tree, target_names=['0', '1']))
    print()
    tree_clf = tree.DecisionTreeClassifier(**tree_params)
    clf_tree = tree_clf.fit(X_train,y_train)
    clf_tree_class_name = clf_tree.__class__.__name__
    plot_confusion_matrix_sns(y_pred=y_pred_tree, y_test=y_test,title='{0} confusion matrix'.format(clf_tree_class_name),threshold=0.5,path=img_path)
    pic_tree(clf=clf_tree,fcols=X_train.columns,title="{0} Structure".format(clf_tree_class_name),file_path=img_path)
    pic_performence(clf=clf_tree,X_train=X_train,y_train=y_train,X_test=X_test,y_test=y_test,
                    file_path=img_path)
    pic_feature_importance(clf=clf_tree,cols=X_train.columns,title="{0} Feature Importance".format(clf_tree_class_name),file_path=img_path)


    print('random froest')
    rf_params, clf_rf_0 = gridCV(clf=RandomForestClassifier, X=X_train_cv, y=y_train_cv,
                             base_params=rf_base_params, all_test_params=rf_test_params, cv_parmas=cv_params)
    y_pred_rf = clf_rf_0.predict(X_test)
    print(classification_report(y_test, y_pred_rf, target_names=['0', '1']))
    print()
    tree_rf = RandomForestClassifier(**rf_params)
    clf_rf= tree_rf.fit(X_train,y_train)
    clf_rf_class_name = clf_rf.__class__.__name__
    plot_confusion_matrix_sns(y_pred=y_pred_rf, y_test=y_test,title='{0} confusion matrix'.format(clf_rf_class_name),threshold=0.5,path=img_path)
    pic_performence(clf=clf_rf,X_train=X_train,y_train=y_train,X_test=X_test,y_test=y_test,
                    file_path=img_path)
    pic_feature_importance(clf=clf_rf,cols=X_train.columns,title="{0} Feature Importance".format(clf_rf_class_name),file_path=img_path)

    if postfix == 0.09:
        joblib.dump(clf_rf, '{0}_random_forest.pkl'.format(os_sys))
        joblib.dump(clf_tree, '{0}_decision_tree.pkl'.format(os_sys))