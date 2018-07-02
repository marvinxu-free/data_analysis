import uuid
import networkx as nx
import numpy as np
import pandas as pd


def online_prec_rec(re):
    """

    Args:
        re:

    Returns:

    """
    # totalPair = sum([i * (i - 1) / 2 for i in re.groupby("brand").size().values])
    originalPair = sum([i * (i - 1) / 2 for i in re.groupby("true_id").size().values])
    predPair = sum([i * (i - 1) / 2 for i in re.groupby("pred_id").size().values])
    rightPair = sum([i * (i - 1) / 2 for i in re.groupby(["true_id", "pred_id"]).size().values])
    print "originalPair: ", originalPair
    print "predPair: ", predPair
    print "rightPair: ", rightPair
    # print "totalPair: ", totalPair
    precision = float(rightPair) / predPair
    recall = float(rightPair) / originalPair
    precision_upper = precision + 2.578 * np.sqrt(precision * (1 - precision) / (predPair))
    precision_lower = precision - 2.578 * np.sqrt(precision * (1 - precision) / (predPair))
    recall_upper = recall + 2.578 * np.sqrt(recall * (1 - recall) / originalPair)
    recall_lower = recall - 2.578 * np.sqrt(recall * (1 - recall) / originalPair)
    print "precision: ", precision, [precision_lower, precision_upper]
    print "recall: ", recall, [recall_lower, recall_upper]
    tpr = float(rightPair) / originalPair
    print "tpr:", tpr
    # fpr = float(predPair - rightPair) / (totalPair - originalPair)
    # print "fpr:", fpr
    return precision, recall, precision_lower, recall_lower, precision_upper, recall_upper, tpr


def do_online_prec_rec(data):
    """

    Args:
        data: dataframe including prob, pred, label, ts_diff, event_id

    Returns:

    """

    originalEdge = getEdge(data[data["label"] == 1][["query_event_id", "doc_event_id"]])
    eventSet = Set(data["query_event_id"].unique()).union(Set(data["doc_event_id"].unique()))
    trueMaxentID = pd.DataFrame(eventToMaxent(originalEdge, eventSet).items())
    trueMaxentID.columns = ["event_id", "true_id"]
    # queryEventID = data[["query_event_id", "query_brand"]]
    # queryEventID.rename(columns={"query_event_id": "event_id", "query_brand": "brand"}, inplace=True)
    # docEventID = data[["doc_event_id", "doc_brand"]]
    # docEventID.rename(columns={"doc_event_id": "event_id", "doc_brand": "brand"}, inplace=True)
    # eventBrand = pd.concat([queryEventID, docEventID]).drop_duplicates()
    _df = pd.DataFrame({"prob": data.groupby(["query_event_id"])["prob"].max()}).reset_index()
    _df = _df.merge(data, on=["query_event_id", "prob"])
    _re = pd.DataFrame({"ts_diff": _df.groupby(["query_event_id"])["ts_diff"].min()}).reset_index()
    _re = _re.merge(_df, on=["query_event_id", "ts_diff"])
    predEdge = getEdge(_re[_re["pred"] == 1][["query_event_id", "doc_event_id"]])
    predMaxentID = pd.DataFrame(eventToMaxent(predEdge, eventSet).items())
    predMaxentID.columns = ["event_id", "pred_id"]
    re = trueMaxentID.merge(predMaxentID, on=["event_id"])
    # re = re.merge(eventBrand, on=["event_id"])
    print "online_prec_rec"
    return online_prec_rec(re)
    # return online_prec_rec(re), _re


def prec_and_recall(score, y):
    # type: (object, object) -> object
    """
    This function is used to calculate the precision and recall of forecast.

    Parameters
    ----------
    score: predict label
    y: actual label

    Returns
    -------
    precision: the proportion of events which is actually true among those predicted to be true.
    recall: the proportion of events which is predicted to be true among those are actually true.
    """
    truePositive = 0
    falsePositive = 0
    falseNegative = 0
    trueNegative = 0
    for i in np.arange(len(y)):
        if (score[i] == 1) & (y[i] == 1):
            truePositive += 1
        elif (score[i] == 1) & (y[i] == 0):
            falsePositive += 1
        elif (score[i] == 0) & (y[i] == 1):
            falseNegative += 1
        else:
            trueNegative += 1
    # print "truePositive:", truePositive
    # print "falsePositive:", falsePositive
    # print "trueNegative:", trueNegative
    # print "falseNegative:", falseNegative
    if truePositive + falsePositive == 0:
        precision = 1.0
    else:
        precision = truePositive * 1.0 / (truePositive + falsePositive)
    if truePositive + falseNegative == 0:
        recall = 0.0
    else:
        recall = truePositive * 1.0 / (truePositive + falseNegative)
    TPR = truePositive * 1.0 / (truePositive + falseNegative)
    FPR = falsePositive * 1.0 / (falsePositive + trueNegative)
    if precision + recall == 0:
        FScore = 0.0
    else:
        FScore = 2 * precision * recall / (precision + recall)
    # precision_upper = precision + 2.578 * np.sqrt(precision * (1 - precision) / (truePositive + falsePositive))
    # precision_lower = precision - 2.578 * np.sqrt(precision * (1 - precision) / (truePositive + falsePositive))
    # recall_upper = recall + 2.578 * np.sqrt(recall * (1 - recall) / (truePositive + falseNegative))
    # recall_lower = recall - 2.578 * np.sqrt(recall * (1 - recall) / (truePositive + falseNegative))
    print "precision: ", precision
    print "recall: ", recall
    # print "f-score:\t", FScore
    return precision, recall, 0.0, 0.0, 1.0, 1.0, TPR, FPR, FScore
    # return precision, recall, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0
    # return precision, recall, TPR, FPR


def eventToMaxent(edge, node):
    """
    """
    G = nx.Graph()
    G.add_nodes_from(node)
    G.add_edges_from(edge)
    maxentIdHashMap = {}
    for i in nx.connected_components(G):
        maxent_id = str(uuid.uuid4())
        for j in i:
            maxentIdHashMap[j] = maxent_id
    return maxentIdHashMap


def getEdge(data):
    """
    """
    return [tuple(i) for i in data.values]