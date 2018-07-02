# -*- coding: utf-8 -*-
from poc环境模型研究.dfp_modeling.read_online_data import read_online_data
from poc环境模型研究.dfp_modeling.do_evaluation import do_evaluation


def run_online():
    # android
    # app_app = read_online_data("./app_text/part-00000")
    # app_web = read_online_data("./app_web_text/part-00000")
    # ios
    app_app = read_online_data("/Users/Ping.Ji/app_app_text_ios/part-00000")
    app_app = app_app.sample(frac=0.25, random_state=40)
    # app_web = read_online_data("/Users/Ping.Ji/app_web_text_ios/part-00000")
    # print "label=1:", len(app_web[(app_web.label == 1)])
    # print "label=1 and pred=1:", len(app_web[(app_web.label == 1) & (app_web.prob >= 0.5)])
    # print "pred=1:", len(app_web[(app_web.prob >= 0.5)])
    # app_web["doc_event_id"] = map(lambda x: x + "_new", app_web["doc_event_id"])
    # app_web["query_event_id"] = map(lambda x: x + "_new", app_web["query_event_id"])
    # app_web_sample = app_web.sample(n=9886, random_state=40)   # app_app: app_web =
    # app_web_sample = app_web.sample(n=36337, random_state=40)
    # web-web
    # dfp_df = pd.concat([app_app, app_web_sample])
    # do_evaluation(dfp_df, "dfp_only_web_web_ios")
    # did_df = do_did_match(app_app, 0.9)
    # do_evaluation(did_df, "dfp+did_app_app_ios2")
    # new_df = pd.concat([did_df, app_web_sample])
    # do_evaluation(new_df, "dfp+did_web_web")
    print len(app_app)  # 454604   # 454223
    # print len(app_web)  # 1069142   # 1816891
    # print len(new_df)   # 731912
    do_evaluation(app_app, "dfp_only_app_app_ios2")
    # do_evaluation(app_web, "dfp_only_app_web_ios")