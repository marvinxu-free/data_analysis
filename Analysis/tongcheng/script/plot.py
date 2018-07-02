# -*- coding: utf-8 -*-
from __future__ import division
import pickle
from matplotlib import pyplot as plt
from sklearn import metrics
import sys
from Pic.maxent_style import maxent_style,remove_palette
import numpy as np

def prc_curve_with_confid(alpha, prec, rec, min_prec, min_rec, max_prec, max_rec):
    """
    This function is to plot the precision and recall corresponding to different alpha.

    Parameters
    ----------
    alpha:
    avg_prec: average precision of k-fold
    avg_rec: average recall of k-fold
    min_prec: minimal precision of k-fold
    min_rec: minimal recall of k-fold
    max_prec: maximum precision of k-fold
    max_rec: maximum recall of k-fold

    Returns
    -------
    no return

    """
    fig = plt.figure(figsize=(12, 8), dpi=80)
    ax = fig.add_subplot(111)
    ax.plot(alpha, prec, 'b-', label='precision')
    ax.plot(alpha, rec, 'r-', label='recall')
    ax.plot(alpha, min_prec, 'b--', label='min_precision')
    ax.plot(alpha, min_rec, 'r--', label='min_recall')
    ax.plot(alpha, max_prec, 'b+', label='max_precision')
    ax.plot(alpha, max_rec, 'r+', label='max_recall')
    ax.set_ylabel("prec/recall")
    ax.set_xlim(0.5, 1.0)
    ax.set_ylim(0.0, 1.2)
    ax.legend(loc='best')
    plt.title("Precision and recall of " + sys.argv[2])
    plt.show()


# def prc_curve(alpha, prec, rec, min_prec, min_rec, max_prec, max_rec):
#     """
#     This function is to plot the precision and recall corresponding to different alpha.
#
#     Parameters
#     ----------
#     alpha:
#     avg_prec: average precision of k-fold
#     avg_rec: average recall of k-fold
#     min_prec: minimal precision of k-fold
#     min_rec: minimal recall of k-fold
#     max_prec: maximum precision of k-fold
#     max_rec: maximum recall of k-fold
#
#     Returns
#     -------
#     no return
#
#     """
#     fig = plt.figure(figsize=(12, 8), dpi=80)
#     ax = fig.add_subplot(311)
#     # ax.set_title("%s" % "")
#     ax.plot(alpha, prec, 'b-', label='average_precision')
#     ax.plot(alpha, rec, 'r-', label='average_recall')
#     ax.set_ylabel("prec/recall")
#     ax.set_xlim(0.0, 1.0)
#     ax.set_ylim(0.0, 1.2)
#     ax.legend(loc='best')
#     os = sys.argv[2]
#     plt.title("Precision and recall of " + os)
#     ax = fig.add_subplot(312)
#     ax.plot(alpha, min_prec, 'b--', label='min_precision')
#     ax.plot(alpha, min_rec, 'r--', label='min_recall')
#     ax.set_xlim(0.0, 1.0)
#     ax.set_ylim(0.0, 1.2)
#     ax.set_ylabel("prec/recall")
#     ax.legend(loc='best')
#     ax = fig.add_subplot(313)
#     ax.plot(alpha, max_prec, 'b+', label='max_precision')
#     ax.plot(alpha, max_rec, 'r+', label='max_recall')
#     ax.set_xlabel("alpha")
#     ax.set_ylabel("prec/recall")
#     ax.set_xlim(0.0, 1.0)
#     ax.set_ylim(0.0, 1.2)
#     ax.legend(loc='best')
#     plt.show()


@maxent_style
def prc_curve(alpha, prec, rec, model_name,file_path,palette=None):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(alpha, prec, color=next(palette),marker="o",ms=3,label='precision')
    ax.plot(alpha, rec, color=next(palette),marker="o",ms=3,label='recall')
    ax.set_xlabel("alpha")
    ax.set_ylabel("prec/recall")
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.2)
    ax.legend(loc='best')
    title = "precision and recall for different alpha(%s)" % model_name
    ax.set_title(title)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    file_path += "/{0}.png".format(title.replace(" ","_"))
    fig.savefig(filename=file_path,format='png')
    plt.show(block=False)


def prec_rec_curve(prec1, rec1, prec2, rec2, prec3, rec3, os):
    fig = plt.figure(figsize=(12, 8), dpi=80)
    ax = fig.add_subplot(111)
    ax.scatter(prec1, rec1, c='y', marker='o', label='mcid_match')
    ax.scatter(prec2, rec2, c='c', marker='o', label='mcid+did_match')
    ax.set_xlabel("precision")
    ax.set_ylabel("recall")
    ax.annotate('mcid, did and dfp match', (0.5, 1.02))
    ax.annotate('mcid match', (prec1 + 0.01, rec1 + 0.01))
    ax.annotate('mcid and did match', (prec2 + 0.01, rec2 - 0.01))
    ax.plot(prec3, rec3, 'b', label='mcid+did+dfp_match')
    ax.legend(loc='lower left')
    plt.title("Precision vs recall of " + os)
    plt.show()


@maxent_style
def prc_curve_cmp(alpha,file_path,palette=None,scale_start=0.9, pr_arr=[]):
    fig,axes = plt.subplots(1,2,figsize=(12, 8), dpi=80)
    index = (alpha >= scale_start)
    for model, precsion, recall in pr_arr:
        color = next(palette)
        axes[0].plot(alpha, precsion, color=color,linestyle="-",marker="+",label='{0} precision'.format(model))
        axes[0].plot(alpha, recall, color=color,linestyle="-",marker="*",label='{0} recall'.format(model))
        axes[1].plot(alpha[index], precsion[index], color=color,linestyle="-",marker="+",label='{0} precision'.format(model))
        axes[1].plot(alpha[index], recall[index], color=color,linestyle="-",marker="*",label='{0} recall'.format(model))
    axes[0].set_ylabel("prec/recall")
    axes[0].set_xlabel("alpha")
    axes[0].set_xlim(0.0, 1.0)
    axes[0].set_ylim(0.0, 1.1)
    axes[0].legend(loc='best')
    axes[0].set_title("Precision and Recall Totally")
    axes[0].spines['right'].set_visible(False)
    axes[0].spines['top'].set_visible(False)
    axes[1].set_ylabel("prec/recall")
    axes[1].set_xlabel("alpha")
    axes[1].set_xlim(scale_start, 1.0)
    axes[1].set_ylim(0.0, 1.1)
    axes[1].legend(loc='best')
    axes[1].set_title("Precision and Recall: alpha>={0}".format(scale_start))
    axes[1].spines['right'].set_visible(False)
    axes[1].spines['top'].set_visible(False)
    file_path += "/{0}.png".format("Precision and Recall".replace(" ","_"))
    fig.savefig(filename=file_path,format='png')
    plt.show(block=False)


@maxent_style
def prc_total_cmp(file_path,palette=None,scale_start=0.95,  pr_dict={}):
    fig,axes = plt.subplots(1,2,figsize=(12, 8), dpi=80)
    alphas = np.arange(0,1.0,0.01)
    index = np.where(abs(alphas - scale_start) < 1e-6)[0][0]
    app_ratio = pr_dict.pop('ratio')
    web_array = np.array(pr_dict['web-web'])
    app_array = np.array(pr_dict['app-app'])
    pr_array = []
    for ratio in alphas:
        pr_array.append(ratio * app_array + (1-ratio) * web_array)
    pr_array = np.array(pr_array)
    color1 = next(palette)
    color2 = next(palette)
    color3 = next(palette)
    axes[0].plot(alphas, pr_array[:,0,index], color=color1,linestyle="-",marker="+",label='{0} precision'.format("xgboost"))
    axes[0].plot(alphas, pr_array[:,1,index], color=color1,linestyle="-",marker="*",label='{0} recall'.format("xgboost"))
    axes[0].plot(alphas, pr_array[:,2,index], color=color2,linestyle="-",marker="+",label='{0} precision'.format("did"))
    axes[0].plot(alphas, pr_array[:,3,index], color=color2,linestyle="-",marker="*",label='{0} recall'.format("did"))
    axes[0].plot(alphas, pr_array[:,4,index], color=color3,linestyle="-",marker="+",label='{0} precision'.format("did+xgboost"))
    axes[0].plot(alphas, pr_array[:,5,index], color=color3,linestyle="-",marker="+",label='{0} recall'.format("did+xgboost"))

    pr_array = app_ratio * app_array + (1-app_ratio) * web_array
    axes[1].plot(alphas, pr_array[0], color=color1,linestyle="-",marker="+",label='{0} precision'.format("xgboost"))
    axes[1].plot(alphas, pr_array[1], color=color1,linestyle="-",marker="*",label='{0} recall'.format("xgboost"))
    axes[1].plot(alphas, pr_array[2], color=color2,linestyle="-",marker="+",label='{0} precision'.format("did"))
    axes[1].plot(alphas, pr_array[3], color=color2,linestyle="-",marker="*",label='{0} recall'.format("did"))
    axes[1].plot(alphas, pr_array[4], color=color3,linestyle="-",marker="+",label='{0} precision'.format("did+xgboost"))
    axes[1].plot(alphas, pr_array[5], color=color3,linestyle="-",marker="*",label='{0} recall'.format("did+xgboost"))

    axes[0].set_ylabel("prec/recall")
    axes[0].set_xlabel("app ratio")
    axes[0].set_xlim(0.0, 1.0)
    axes[0].set_ylim(0.0, 1.1)
    axes[0].legend(loc='best')
    axes[0].set_title(u"APP/WEB准确率与召回率随比例变化(alpha={0:.2f}".format(scale_start))
    axes[0].spines['right'].set_visible(False)
    axes[0].spines['top'].set_visible(False)
    axes[1].set_ylabel("prec/recall")
    axes[1].set_xlabel("alpha")
    axes[1].set_xlim(0.0, 1.0)
    axes[1].set_ylim(0.0, 1.1)
    axes[1].legend(loc='best')
    axes[1].set_title(u"APP/WEB准确率与召回率随阈值变化(APP/总体:{0:.2f})".format(app_ratio))
    axes[1].spines['right'].set_visible(False)
    axes[1].spines['top'].set_visible(False)
    file_path += "/{0}.png".format("APP+WEB Precision and Recall".replace(" ","_"))
    fig.savefig(filename=file_path,format='png')
    plt.show(block=False)

def do_plot_prc():
    alpha_list = pickle.load(open("ios_alpha_list", "rb"))
    ios_dfp_only_prec = pickle.load(open("ios_dfp_only_prec", "rb"))
    ios_dfp_only_rec = pickle.load(open("ios_dfp_only_rec", "rb"))
    ios_dfp_did_prec = pickle.load(open("ios_dfp_did_prec", "rb"))
    ios_dfp_did_rec = pickle.load(open("ios_dfp_did_rec", "rb"))
    prc_curve_cmp(alpha_list, ios_dfp_only_prec, ios_dfp_only_rec, ios_dfp_did_prec, ios_dfp_did_rec, 'ios')
    mcid_prec = 1.0
    mcid_rec = 0.0805369127517
    mcid_did_prec = 1.0
    mcid_did_rec = 0.0805369127517
    prec_rec_curve(mcid_prec, mcid_rec, mcid_did_prec, mcid_did_rec, ios_dfp_did_prec, ios_dfp_did_rec, 'ios')
    alpha_list = pickle.load(open("android_alpha_list", "rb"))
    android_dfp_only_prec = pickle.load(open("android_dfp_only_prec", "rb"))
    android_dfp_only_rec = pickle.load(open("android_dfp_only_rec", "rb"))
    android_dfp_did_prec = pickle.load(open("android_dfp_did_prec", "rb"))
    android_dfp_did_rec = pickle.load(open("android_dfp_did_rec", "rb"))
    mcid_prec = 1.0
    mcid_rec = 0.0925492816572
    mcid_did_prec = 1.0
    mcid_did_rec = 0.0925492816572
    prc_curve_cmp(alpha_list, android_dfp_only_prec, android_dfp_only_rec, android_dfp_did_prec, android_dfp_did_rec,
                  'android')
    prec_rec_curve(mcid_prec, mcid_rec, mcid_did_prec, mcid_did_rec, android_dfp_did_prec, android_dfp_did_rec,
                   'android')


def visualizeRoc(fpr, tpr, model_name):
    """
    This function is to plot the roc curve.

    Parameters
    ----------
    fpr: false positive rate corresponding to different alpha
    tpr: true positive rate corresponding to different alpha
    auc: area under curve

    Returns
    -------
    no return

    """
    fig = plt.figure(figsize=(6, 6), dpi=80)
    ax = fig.add_subplot(111)
    ax.plot(fpr, tpr)
    auc = metrics.auc(fpr, tpr)
    ax.set_title("auc:%f" % auc)
    ax.fill_between(fpr, tpr)
    ax.set_xlabel("false positive rate")
    ax.set_ylabel("true positive rate")
    plt.title("auc curve for %s(auc=%f)" % (model_name, auc))
    plt.show()


def do_plot_roc():
    tpr1 = pickle.load(open("android_dfp_only_tpr", "rb"))
    print tpr1
    fpr1 = pickle.load(open("android_dfp_only_fpr", "rb"))
    print fpr1
    tpr2 = pickle.load(open("android_mcid_did_dfp_tpr", "rb"))
    print tpr2
    fpr2 = pickle.load(open("android_mcid_did_dfp_fpr", "rb"))
    print fpr2
    tpr3 = 0.0925492816572
    fpr3 = 0.0
    tpr4 = 0.0925492816572
    fpr4 = 0.0
    roc_curve_cmp(fpr1, tpr1, fpr2, tpr2, fpr3, tpr3, fpr4, tpr4)


# def roc_curve_cmp(fpr1, tpr1, fpr2, tpr2, fpr3, tpr3, fpr4, tpr4):
#     fig = plt.figure(figsize=(6, 6), dpi=80)
#     ax = fig.add_subplot(1, 1, 1)
#     ax.set_xlim([-0.1, 1.1])
#     ax.set_ylim([-0.1, 1.1])
#     auc1 = metrics.auc(fpr1, tpr1)
#     ax.plot(fpr1, tpr1, "k--", label='dfp_match(area = %0.4f)' % auc1)
#     auc2 = metrics.auc(fpr2, tpr2)
#     ax.plot(fpr2, tpr2, "b-.", label='mcid+did+dfp_match(area = %0.4f)' % auc2)
#     ax.scatter(fpr3, tpr3, c='r', marker='o', label='mcid_match')
#     ax.scatter(fpr4, tpr4, c='g', marker='o', label='mcid+did_match')
#     plt.xlabel("False Positive Rate")
#     plt.ylabel("True Positive Rate")
#     ax.legend(loc='lower right', shadow=True)
#     plt.title("Receiver operating characteristic")
#     plt.show()


# def roc_curve_cmp(fpr1, tpr1, fpr2, tpr2):
#     fig = plt.figure(figsize=(12, 8), dpi=80)
#     ax = fig.add_subplot(1, 1, 1)
#     ax.set_xlim([-0.1, 1.1])
#     ax.set_ylim([-0.1, 1.1])
#     auc1 = metrics.auc(fpr1, tpr1)
#     ax.plot(fpr1, tpr1, "b-", label='old_model(area = %0.4f)' % auc1)
#     auc2 = metrics.auc(fpr2, tpr2)
#     ax.plot(fpr2, tpr2, "r+", label='new_model(area = %0.4f)' % auc2)
#     plt.xlabel("False Positive Rate")
#     plt.ylabel("True Positive Rate")
#     ax.legend(loc='lower left', shadow=True)
#     plt.title("Receiver operating characteristic")
#     plt.show()


@maxent_style
def roc_curve_cmp(file_path,palette=None,roc_arr=[]):
    fig = plt.figure(figsize=(12, 8), dpi=80)
    ax = fig.add_subplot(1, 1, 1)
    for model,fpr,tpr in roc_arr:
        auc = metrics.auc(fpr, tpr)
        ax.plot(fpr, tpr, color=next(palette), label='{0} AUC = {1:.4f})'.format(model,auc))
    ax.set_xlim([-0.1, 1.1])
    ax.set_ylim([-0.1, 1.1])
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.legend(loc='lower left', shadow=True)
    title = "Comparison of roc curve"
    ax.set_title(title)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    file_path += "/{0}.png".format(title.replace(" ","_"))
    fig.savefig(filename=file_path,format='png')
    plt.show(block=False)


# def fscore_cmp(alpha_list, fscore_list, fscore_list_gbdt):
#     plt.figure(figsize=(12, 8), dpi=80)
#     plt.plot(alpha_list, fscore_list, 'b-', label="old_model")
#     plt.plot(alpha_list, fscore_list_gbdt, 'r+', label="new_model")
#     plt.legend(loc="best")
#     plt.xlim([-0.1, 1.1])
#     plt.ylim([0.3, 1.1])
#     plt.title("fscore comparison between old and new model")
#     plt.xlabel("alpha")
#     plt.ylabel("fscore")
#     # plt.hist(data["ts_diff"], bins=20)
#     plt.show()


@maxent_style
def fscore_cmp(alpha,file_path,palette=None, pr_arr=[]):
    fig = plt.figure(figsize=(12, 8), dpi=80)
    ax = fig.add_subplot(111)
    for model, fscores in pr_arr:
        ax.plot(alpha, fscores, color=next(palette),marker='+',label="{0}".format(model))
    ax.legend(loc="best")
    ax.set_xlim([-0.1, 1.1])
    ax.set_ylim([0.3, 1.1])
    ax.set_xlabel("alpha")
    ax.set_ylabel("f-score")
    title = "Comparison of f-score"
    ax.set_title(title)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    file_path += "/{0}.png".format(title.replace(" ","_"))
    fig.savefig(filename=file_path,format='png')
    plt.show(block=False)



@maxent_style
def plot_prc_by_time(file_path,time_list, palette=None,prc_time=[]):
    fig = plt.figure(figsize=(12, 8), dpi=80)
    ax = fig.add_subplot(111)
    for model,prec,rec in prc_time:
        color = next(palette)
        ax.plot(time_list, prec, color=color,label="{0} precision".format(model))
        ax.plot(time_list, rec, color=color,marker="+", label="{0} recall".format(model))
    plt.legend(loc="best")
    ax.set_ylim([0.0, 1.1])
    title = "Comparison of precision and recall over time"
    ax.set_xlabel("min")
    ax.set_ylabel("prec/recall")
    ax.set_title(title)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    file_path += "/{0}.png".format(title.replace(" ","_"))
    fig.savefig(filename=file_path,format='png')
    plt.show(block=False)
