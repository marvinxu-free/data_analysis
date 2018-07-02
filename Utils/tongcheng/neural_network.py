import pandas as pd
from sklearn.neural_network import MLPClassifier
from do_evaluation import evaluate_result
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler

def train_nn(train_set, cols):
    x = train_set[cols]
    y = train_set["label"]
    # clf = MLPClassifier(activation='logistic', solver='sgd', alpha=1e-5, tol=1e-6, momentum=1., hidden_layer_sizes=(10, 2),
    #                     learning_rate='constant', random_state=101, max_iter=1000)
    # neuron1 = int(sys.argv[2])
    # neuron2 = int(sys.argv[3])
    # mmt = float(sys.argv[4])
    clf = MLPClassifier(solver='sgd', alpha=1e-5, momentum=1.0,
                        random_state=101, max_iter=1000, activation='tanh',
                        learning_rate='constant', learning_rate_init=0.01, tol=1e-04)
    # clf = MLPClassifier(solver='sgd', alpha=1e-5, momentum=1.0,
    #                     random_state=101, max_iter=1000, activation='tanh', hidden_layer_sizes=(100,),
    #                     learning_rate='constant', learning_rate_init=0.01, tol=1e-04)
    param_grid = {"hidden_layer_sizes": [(), (10,), (15,), (20,), (100,),
                  (10, 2), (20, 2), (100, 2), (10, 5, 2)]}
    grid = GridSearchCV(clf, param_grid=param_grid, n_jobs=1)
    grid.fit(x, y)
    print("Best: %f using %s" % (grid.best_score_, grid.best_params_))
    # clf.fit(x, y)
    return grid


def test_nn(clf, test_set, cols):
    x = test_set[cols]
    y = test_set["label"]
    proba = clf.predict_proba(x)[:, 1]
    alpha_list, prec, rec, fscore, tpr, fpr = evaluate_result(proba, y.values)
    print "precision:", prec[90], " recall:", rec[90], " fscore:", fscore[90]
    # print prob[:2]
    # print "----------"
    # print np.max(prob[:, 1])
    return alpha_list, prec, rec, fscore, tpr, fpr

def do_nn(train_set, test_set, cols):
    scaler = StandardScaler()
    scaler.fit(train_set[cols])
    train_set[cols] = scaler.transform(train_set[cols])
    test_set[cols] = scaler.transform(test_set[cols])
    clf = train_nn(train_set, cols)