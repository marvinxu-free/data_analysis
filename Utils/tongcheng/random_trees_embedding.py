from sklearn.ensemble import *
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import GridSearchCV
from do_evaluation import evaluate_result


def train_rte(train_set, cols):
    x = train_set[cols]
    y = train_set["label"]
    rt = RandomTreesEmbedding(max_depth=3, random_state=20, n_estimators=200)
    rt_lm = LogisticRegression(fit_intercept=True, class_weight='balanced', solver='newton-cg', max_iter=1000, C=0.01)
    pipeline = make_pipeline(rt, rt_lm)
    # print pipeline.get_params().keys()
    pipeline.fit(x, y)
    # grid = GridSearchCV(pipeline, param_grid={"logisticregression__solver": ['lbfgs', 'sag', 'liblinear', 'newton-cg'],
    #                                           "logisticregression__max_iter": [100, 1000],
    #                                           "logisticregression__C": [0.01, 0.1, 1.0],
    #                                           "randomtreesembedding__n_estimators": [10, 50, 100, 200]})
    # grid.fit(x, y)
    # print ("Best: %f using %s" % (grid.best_score_, grid.best_params_))
    return pipeline


def test_rte(clf, test_set, cols):
    x = test_set[cols]
    y = test_set["label"]
    proba = clf.predict_proba(x)[:, 1]
    alpha_list, prec, rec, fscore, tpr, fpr = evaluate_result(proba, y.values)
    return alpha_list, prec, rec, fscore, tpr, fpr


def do_rte(train_set, test_set, cols):
    clf = train_rte(train_set, cols)
    alpha_list, prec, rec, fscore, tpr, fpr = test_rte(clf, test_set, cols)
    return alpha_list, prec, rec, fscore, tpr, fpr
