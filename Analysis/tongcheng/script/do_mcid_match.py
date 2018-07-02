import pandas as pd
from dfp_model.prec_rec import do_online_prec_rec


def mcid_match(df):
    """
    This function is to do mcid(for app) and ckid(for web) match
    Args:
        df:

    Returns:

    """
    df["mcid_equals"] = (df["query_mcid"] == df["doc_mcid"]) + 0
    df["ckid_equals"] = (df["query_ckid"] == df["doc_ckid"]) + 0
    df["mcid_match_label"] = map(lambda x, y: 1 if x + y >= 1 else 0, df.mcid_equals, df.ckid_equals)
    df["prob_tmp"] = map(lambda x: 1.0 if x == 1 else 0.0, df.mcid_match_label)
    df["pred_tmp"] = map(lambda x: 1 if x == 1 else 0, df.mcid_match_label)
    if "prob" in df.columns:
        df["prob"] = df[["prob_tmp", "prob"]].max(axis=1)
        df["pred"] = df[["pred_tmp", "pred"]].max(axis=1)
    else:
        df["prob"] = df["prob_tmp"]
        df["pred"] = df["pred_tmp"]
    pd.set_option("max_colwidth", 300)
    do_online_prec_rec(df)
    return df
