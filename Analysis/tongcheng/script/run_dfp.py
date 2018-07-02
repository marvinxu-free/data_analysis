from Utils.tongcheng.data_reader import read_dfp_details
from Utils.tongcheng.xg_boost import *
from Utils.tongcheng.do_did_and_xgboost import do_did_and_xgboost,do_did
from plot import *
from Params.path_params import Data_path
import os
# from sklearn.model_selection import train_test_split
from Utils.common.splitData import splitData
import errno
from Pic.hist import pic_label

# feature_cols = ["base_entropy", "boottime_entropy", "browser_entropy", "ip_entropy", "ua_entropy"]
feature_cols1 = ["boottime_entropy", "ip_entropy", "user_agent_entropy"]
# feature_cols = ["base_entropy", "boottime_entropy", "browser_entropy", "ip_entropy", "ua_entropy", "ts_diff", "slope"]
feature_cols2 = ["boottime_entropy", "city_entropy",
        "client_ts_entropy",     "device_entropy",         "ip_entropy",
        "ip_seg_16_entropy",  "ip_seg_24_entropy", "device_browser_engine_entropy", "device_osversion_entropy",
            "province_entropy",  "slope", "user_agent_entropy",
               "tcpts_diff"]


def print_max_fscore(alpha_list, prec_list, rec_list, fscore):
    print "max fscore is:", max(fscore)
    print "alpha is:", alpha_list[max((v, i) for i, v in enumerate(fscore))[1]]
    print "precision is:", prec_list[max((v, i) for i, v in enumerate(fscore))[1]]
    print "recall is:", rec_list[max((v, i) for i, v in enumerate(fscore))[1]]


def main(fname):
    data = read_dfp_details(fname)
    data = data.fillna(0)
    data_all_zero = data.columns[(data == 0).all()]
    print("all zero columns is {0}".format(data_all_zero))
    total_pr = {}
    app_app_pr = dfp_main(data=data,mode="app-app")
    web_web_pr = dfp_main(data=data,mode="web-web")
    # app_web_pr = dfp_main(data=data,mode="app-web")
    total_pr.update(app_app_pr)
    total_pr.update(web_web_pr)
    total_pr['ratio'] = 1.0 * data[data.match_type == 2].shape[0] / data.shape[0]
    img_path = "{0}/image_tongcheng".format(Data_path)
    prc_total_cmp(file_path=img_path,pr_dict=total_pr)


def dfp_main(data, mode='app-web'):
    img_path = "{0}/image_tongcheng/{1}".format(Data_path,mode)
    print("image path {0}".format(img_path))
    if not os.path.exists(img_path):
        try:
            os.makedirs(img_path)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
    print len(data[(data.label == 1)])
    print len(data[(data.label == 0)])
    print "app_web positive:", len(data[(data.match_type == 1) & (data.label == 1)])
    print "app_web negative:", len(data[(data.match_type == 1) & (data.label == 0)])
    print "app_app positive:", len(data[(data.match_type == 2) & (data.label == 1)])
    print "app_app negative:", len(data[(data.match_type == 2) & (data.label == 0)])
    print "web_web positive:", len(data[(data.match_type == 0) & (data.label == 1)])
    print "web_web negative:", len(data[(data.match_type == 0) & (data.label == 0)])

    if mode == "app-web":
        data = data[(data.match_type == 1)] # best hyperparameter: n_estimators=80, max_depth=3, c=1.0  less features
    elif mode == "app-app":
        data = data[(data.match_type == 2)] # best hyperparameter: n_estimators=150, max_depth=3, c=0.001  more features
    elif mode == "web-web":
        data = data[(data.match_type == 0)]
    else:
        print('wrong match type')
        exit(1)
    pic_label(df=data,col='label',title="{0} label ratio".format(mode),file_path=img_path)
    train_set, test_set = splitData(data,ratio=0.7)
    print len(test_set[(test_set.label == 0)])
    print len(test_set[(test_set.label == 1)])

    model_name = '{0}_xgb.model'.format(mode)

    alpha_list_xgb, prec_list_xgb, rec_list_xgb, fscore_list_xgb, tpr_list_xgb, fpr_list_xgb, time_series_xgb, prec_series_xgb,rec_series_xgb = do_xgboost(train_set, test_set, feature_cols2,model_name=model_name)

    # prc_curve(alpha_list_xgb, prec_list_xgb, rec_list_xgb, "xgboost",file_path=img_path)

    alpha_list, prec_list, rec_list, fscore_list, tpr_list, fpr_list, time_series, prec_series, rec_series = do_did(test_set)
    print prec_series[:2]
    # prc_curve(alpha_list, prec_list, rec_list, "did model",file_path=img_path)
    alpha_list_mix, prec_list_mix, rec_list_mix, fscore_list_mix, tpr_list_mix, fpr_list_mix, time_series_mix, prec_series_mix, rec_series_mix = do_did_and_xgboost(test_set=test_set,cols=feature_cols2,mode=mode)

    prc_cmp = [
        ("xgboost", np.array(prec_list_xgb),np.array(rec_list_xgb)),
        ("did", np.array(prec_list),np.array(rec_list)),
        ("did+xgboost", np.array(prec_list_mix),np.array(rec_list_mix))
    ]
    prc_curve_cmp(np.array(alpha_list),file_path=img_path,pr_arr=prc_cmp)

    # # visualizeRoc(fpr_list, tpr_list, "old model")
    # roc_cmp = [
    #     ("xgboost", fpr_list_xgb,tpr_list_xgb),
    #     ("did", fpr_list,tpr_list)
    # ]
    # roc_curve_cmp(file_path=img_path,roc_arr=roc_cmp)

    # fscore_cmps = [
    #     ("xgboost", fscore_list_xgb),
    #     ("did", fscore_list)
    # ]
    # fscore_cmp(alpha_list, file_path=img_path,pr_arr=fscore_cmps)

    # prc_time_cmps = [
    #     ("xgboost", prec_series_xgb, rec_series_xgb),
    #     ("did", prec_series, rec_series)
    # ]
    # plot_prc_by_time(time_list=time_series, file_path=img_path,prc_time=prc_time_cmps)

    print "xgboost dfp:"
    print_max_fscore(alpha_list_xgb, prec_list_xgb, rec_list_xgb, fscore_list_xgb)
    print "online dfp:"
    print_max_fscore(alpha_list, prec_list, rec_list, fscore_list)
    return {mode: [np.array(prec_list_xgb),np.array(rec_list_xgb),np.array(prec_list),np.array(rec_list),np.array(prec_list_mix),np.array(rec_list_mix)]}


def analyse():
    fname = sys.argv[1]
    data = read_dfp_details(fname)
    data = data.fillna(0)
    ip_ratio = []
    ip16_ratio = []
    ip24_ratio = []
    device_ratio = []
    device_browser_engine_ratio = []
    device_osversion_ratio = []
    ua_ratio = []
    for i in np.arange(1, 24 * 60 / 30 + 1):
        # test_set["proba"] = y_prob
        sub_df_copy = data[(data.ts_diff <= i * 30 * 60) & (data.ts_diff > (i - 1) * 30 * 60)]
        print "%f min" % (i*30.0)
        print len(sub_df_copy)
        print len(sub_df_copy[(sub_df_copy.ip_entropy == 0)])
        ip_rat = len(sub_df_copy[(sub_df_copy.ip_entropy == 0)]) * 1.0 / len(sub_df_copy)
        ip16_rat = len(sub_df_copy[(sub_df_copy.ip_seg_16_entropy == 0)]) * 1.0 / len(sub_df_copy)
        ip24_rat = len(sub_df_copy[(sub_df_copy.ip_seg_24_entropy == 0)]) * 1.0 / len(sub_df_copy)
        device_rat = len(sub_df_copy[(sub_df_copy.device_entropy == 0)]) * 1.0 / len(sub_df_copy)
        device_browser_engine_rat = len(sub_df_copy[(sub_df_copy.device_browser_engine_entropy == 0)]) * 1.0 / len(sub_df_copy)
        device_osversion_rat = len(sub_df_copy[(sub_df_copy.device_osversion_entropy == 0)]) * 1.0 / len(sub_df_copy)
        ua_rat = len(sub_df_copy[(sub_df_copy.user_agent_entropy == 0)]) * 1.0 / len(sub_df_copy)
        ip_ratio.append(ip_rat)
        ip16_ratio.append(ip16_rat)
        ip24_ratio.append(ip24_rat)
        device_ratio.append(device_rat)
        device_browser_engine_ratio.append(device_browser_engine_rat)
        device_osversion_ratio.append(device_osversion_rat)
        ua_ratio.append(ua_rat)
    plt.plot(ip_ratio)
    plt.title("ip")
    plt.show()
    plt.plot(ip16_ratio)
    plt.title("ip16")
    plt.show()
    plt.plot(ip24_ratio)
    plt.title("ip24")
    plt.show()
    plt.plot(device_ratio)
    plt.title("device")
    plt.show()
    plt.plot(device_browser_engine_ratio)
    plt.title("device_browser_engine")
    plt.show()
    plt.plot(device_osversion_ratio)
    plt.title("device_osversion")
    plt.show()
    plt.plot(ua_ratio)
    plt.title("ua")
    plt.show()


if __name__ == '__main__':
    file_ = "/Users/chaoxu/code/local-spark/Data/tongcheng/sample_1018/"
    main(fname=file_)

    # analyse()
