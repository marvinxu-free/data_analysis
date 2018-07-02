# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/18
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to pic id anomaly figures based excel
"""
import matplotlib as mpl

mpl.rcParams['agg.path.chunksize'] = 10000
from Utils.risk_feature.risk_feature_for_ecahrts import get_city_cluster_id
from Utils.risk_feature.feature_group_nunique import feature_group_nunique
from Pic.hist import makeFeatureGroupTimeHist, hist_two_fig
from Pic.pic_line import pic_line_threshold_user_ratio
from Pic.pic_pie import pic_pie
from Params.risk_feature_params import ACTIVIT_ID, LABEL_FIELD


def pic_id_anomaly(df, img_path):
    """
    get anomaly images and save to img_path
    :param df:
    :param img_path:
    :return:
    """
    # ----ID异常-----
    get_city_cluster_id(df=df, json_path=img_path)

    df_mobile_ckid = feature_group_nunique(gcol='mobile',
                                           acol=['ckid', 'mobile'],
                                           df=df
                                           )
    hist_two_fig(df=df_mobile_ckid,
                 col='ckid',
                 fname=img_path + "/group_mobile_ckid_state.png",
                 title="同一设备关联的设备指纹ID数量分布",
                 )
    pic_line_threshold_user_ratio(thresholds=df_mobile_ckid['ckid'].values,
                                  users=df_mobile_ckid['mobile'].values,
                                  title="设备指纹ID数量阈值调整与用户占比",
                                  fname=img_path + "/adapt_ckid_based_mobile.png",
                                  )
    pic_pie(values=[df_mobile_ckid.loc[df_mobile_ckid['ckid'] == 1].shape[0],\
                    df_mobile_ckid.loc[df_mobile_ckid['ckid'] > 1].shape[0]],
            cats=['设备指纹ID数量=1', "设备指纹ID数量>1"],
            title="同一设备指纹数量=1与>1比例",
            fname=img_path + "/pie_ckid_based_mobile.png",
            )
    df_ckid_mobile = feature_group_nunique(gcol='ckid',
                                           acol=['mobile'],
                                           df=df
                                           )
    hist_two_fig(df=df_ckid_mobile,
                 col='mobile',
                 fname=img_path + "/group_ckid_UserID_state.png",
                 title="同一设备指纹ID关联用户ID数量分布"
                 )
    pic_pie(values=[df_ckid_mobile.loc[df_ckid_mobile['mobile'] == 1].shape[0],\
                    df_ckid_mobile.loc[df_ckid_mobile['mobile'] > 1].shape[0]],
            cats=['用户ID数量=1', "用户ID数量>1"],
            title="同一设备指纹ID用户ID数量=1与>1比例",
            fname=img_path + "/pie_UserID_based_ckid.png",
            )
    pic_line_threshold_user_ratio(thresholds=df_ckid_mobile['mobile'].values,
                                  users=df_ckid_mobile['mobile'].values,
                                  title="用户ID数量阈值调整与用户占比",
                                  fname=img_path + "/adapt_UserID_based_ckid.png",
                                  )

    df_ckid_event = feature_group_nunique(gcol='ckid',
                                          acol=['event_id', 'mobile'],
                                          df=df,
                                          )
    hist_two_fig(df=df_ckid_event,
                 col='event_id',
                 fname=img_path + "/group_ckid_event_state.png",
                 title="同一设备关联事件数量布"
                 )
    pic_pie(values=[df_ckid_event.loc[df_ckid_event['event_id'] == 1].shape[0], \
                    df_ckid_event.loc[df_ckid_event['event_id'] > 1].shape[0]],
            cats=['事件数量=1', "事件数量>1"],
            title="同一设备指纹ID事件数量=1与>1比例",
            fname=img_path + "/pie_event_based_ckid.png",
            )

    pic_line_threshold_user_ratio(thresholds=df_ckid_event['event_id'].values,
                                  users=df_ckid_event['mobile'].values,
                                  title="事件数量阈值调整与用户占比",
                                  fname=img_path + "/adapt_event_based_ckid.png"
                                  )
    df_ckid_city = feature_group_nunique(gcol='ckid',
                                         acol=['city'],
                                         df=df)
    hist_two_fig(df=df_ckid_city,
                 col='city',
                 title="同一设备指纹ID关联城市数量分布",
                 fname=img_path + "/group_ckid_city_state.png",
                 )
    pic_pie(values=[df_ckid_city.loc[df_ckid_city['city'] == 1].shape[0], \
                    df_ckid_city.loc[df_ckid_city['city'] > 1].shape[0]],
            cats=['城市数量=1', "城市数量>1"],
            title="同一设备指纹ID城市数量=1与>1比例",
            fname=img_path + "/pie_city_based_ckid.png",
            )
    pic_line_threshold_user_ratio(thresholds=df_ckid_city[''].values,
                                  users=df_ckid_city['mobile'].values,
                                  title="城市数量阈值调整与用户占比",
                                  fname=img_path + "/adapt_city_based_ckid.png"
                                  )
    df_ckid_ip = feature_group_nunique(gcol='ckid',
                                       acol='ip',
                                       df=df
                                       )
    hist_two_fig(df=df_ckid_ip,
                 col='ip',
                 title="同一设备指纹ID关联ip数量分布",
                 fname=img_path + "/group_ckid_ip_state.png",
                 )
    pic_pie(values=[df_ckid_ip.loc[df_ckid_ip['ip'] == 1].shape[0],\
                    df_ckid_ip.loc[df_ckid_ip['ip'] > 1].shape[0]],
            cats=['ip数量=1',"ip数量>1"],
            title="同一设备指纹ID ip数量=1与>1比例",
            fname=img_path + "/pie_ip_based_ckid.png",
            )
    pic_line_threshold_user_ratio(thresholds=df_ckid_ip['ip'].values,
                                  users=df_ckid_ip['mobile'].values,
                                  title="ip数量阈值调整与用户占比",
                                  fname=img_path + "/adapt_ip_based_ckid.png"
                                  )
    df_ckid_switch = makeFeatureGroupTimeHist(gcol=['mobile', 'ckid'],
                                              df=df
                                              )
    hist_two_fig(df=df_ckid_switch,
                 col='delta',
                 sparse=False,
                 title="同一设备上设备指纹ID稳定时间分布",
                 fname=img_path + "/group_mobile_ckid_timestamp.png",
                 )
    pic_line_threshold_user_ratio(thresholds=df_ckid_switch['delta'].values,
                                  users=df_ckid_switch['mobile'].values,
                                  fname=img_path + "/adapt_timestamp_delta.png",
                                  reverse=False)
