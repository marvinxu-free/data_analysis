import numpy as np
import tensorflow as tf
from sklearn.model_selection import train_test_split
from tensorflow.python.estimator.inputs import numpy_io


def DNN(X, y, cols):
    X_train, X_test, y_train, y_test = train_test_split(X[cols], y, train_size=0.6)
    X_train = X_train.to_dict(orient="list")
    X_train_ = {}
    for key in X_train.keys():
        X_train_[key] = np.array(X_train[key]).astype(float)
    print X_train_.keys()
    y = y.values
    input_fn = numpy_io.numpy_input_fn(X_train_, y_train, batch_size=2, shuffle=False, num_epochs=1)
    feature_columns = [tf.contrib.layers.real_valued_column(
        ['slope 726.000469512~1000000', 'slope 1.06534147176~276.786553515', u'uaLv3', u'uaLv5',
         'slope 0.0~0.906551360842', u'uaLv6', 'slope 276.786553515~726.000469512',
         'slope 0.906551360842~1.06534147176'], dimension=8)]
    print feature_columns
    classifier = tf.contrib.learn.DNNClassifier(feature_columns=feature_columns, hidden_units=[10, 20, 10], n_classes=2,
                                                model_dir="/tmp/dnn_model")
    classifier.fit(input_fn=input_fn, steps=2000)
    input_fn = numpy_io.numpy_input_fn(X_test, y_test, batch_size=2, shuffle=False, num_epochs=1)
    classifier.evaluate(input_fn=input_fn, steps=1)["accuracy"]
    print("\nTest Accuracy: {0:f}\n".format(accuracy_score))