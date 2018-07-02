from statsmodels import api as sm


def do_stats_model(x, y):
    """
    This model is to test the significance of regression coefficients.

    Parameter
    ---------
    x: the train set of dataframe including the independent variables
    y: the train set of dataframe of label

    """
    Xx = sm.add_constant(x)
    sm_logit = sm.Logit(y, Xx)
    result = sm_logit.fit()
    print result.summary()
    result.pred_table()
    # linear model
    print "linear regression model:\n"
    sm_linear = sm.OLS(y, Xx)
    result = sm_linear.fit()
    print result.summary()