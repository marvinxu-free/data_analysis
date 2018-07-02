# -*- coding: utf-8 -*-
# Project: local-spark
# Author: chaoxu create this file
# Time: 2017/12/18
# Company : Maxent
# Email: chao.xu@maxent-inc.com
"""
this file used to pic device behavior feature analysis in excel
"""
import matplotlib as mpl
mpl.rcParams['agg.path.chunksize'] = 10000
from Pic.hist import makeCitySwitchTimeHist, makeEventSwitchHist, hist_two_fig
from Pic.pic_line import pic_line_threshold_ratio
from Pic.pic_pie import pic_pie


def pic_device_behavior(df, img_path):
    """
    use df to pic and save to img_path
    :param df:
    :param img_path:
    :return:
    """
    # ----设备行为-----
    df_ckid_switch_between_city=makeCitySwitchTimeHist(df=df, gcol='ckid')
    hist_two_fig(df=df_ckid_switch_between_city,
                 col='switch',
                 title="存在异常城市切换行为的设备数量分布",
                 fname=img_path + "/group_ckid_switch_city_state.png",
                 )
    pic_pie(values=[df_ckid_switch_between_city.loc[df_ckid_switch_between_city['switch'] == 0].shape[0], \
                    df_ckid_switch_between_city.loc[df_ckid_switch_between_city['switch'] > 0].shape[0]],
            cats=['异常切换设备数量=0',"异常切换设备数量>0"],
            title="异常切换行为的设备数量=0与>0比例",
            fname=img_path + "/pie_ckid_switch_based_mobile.png",
            )
    pic_line_threshold_ratio(thresholds=df_ckid_switch_between_city['switch'].values,
                             title="设备指纹ID切换过快数量阈值调整与用户占比",
                             fname=img_path + "/adapt_ckid_switch_based_mobile.png",
                             )
    df_event_ckid_same, df_event_ckid_not_same = makeEventSwitchHist(df=df, gcol="mobile", path=img_path)
    if not df_event_ckid_same.empty:
        hist_two_fig(df=df_event_ckid_same,
                     col='same',
                     title="同一设备相同事件类型相邻事件出现不同设备指纹ID",
                     fname=img_path + "/group_same_event_ckid_state.png",
                     )
        pic_pie(values=[df_event_ckid_same.loc[df_event_ckid_same['same'] == 0].shape[0], \
                        df_event_ckid_same.loc[df_event_ckid_same['same'] > 0].shape[0]],
                cats=['不同设备指纹ID数量=0',"不同设备指纹ID数量>0"],
                title="同一设备相同事件类型相邻事件出现不同设备指纹ID数量=0与>0比例",
                fname=img_path + "/pie_same_event_ckid_same.png",
                )
        pic_line_threshold_ratio(thresholds=df_event_ckid_same['same'].values,
                                 title="同一设备相同事件类型相邻事件出现不同设备指纹ID数量调整与用户占比",
                                 fname=img_path + "/adapt_ckid_same_based_mobile.png",
                                 )
    if not df_event_ckid_not_same.empty:
        hist_two_fig(df=df_event_ckid_not_same,
                     col='not_same',
                     title="同一设备不同事件类型相邻事件出现不同设备指纹ID",
                     fname=img_path + "/group_not_same_event_ckid_state.png",
                     )
        pic_pie(values=[df_event_ckid_not_same.loc[df_event_ckid_not_same['not_same'] == 0].shape[0], \
                        df_event_ckid_not_same.loc[df_event_ckid_not_same['not_same'] > 0].shape[0]],
                cats=['不同设备指纹ID数量=0',"不同设备指纹ID数量>0"],
                title="同一设备不同事件类型相邻事件出现不同设备指纹ID数量=0与>0比例",
                fname=img_path + "/pie_not_same_event_ckid_not_same.png",
                )
        pic_line_threshold_ratio(thresholds=df_event_ckid_not_same['not_same'].values,
                                 title="同一设备不同事件类型相邻事件出现不同设备指纹ID数量调整与用户占比",
                                 fname=img_path + "/adapt_ckid_not_same_based_mobile.png",
                                 )

