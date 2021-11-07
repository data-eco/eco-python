"""
Data Summarization Funtionality
KH (Feb 26, 2021)
"""
def compute_summary(path, skip_non_numeric=True):
    """Computes dataframe-wide and row-/column-specific statistics for a dataframe"""
    import numpy as np
    import pandas as pd

    # load data
    dat = pd.read_csv(path, sep="\t")
    dat = dat.set_index(dat.columns[0])

    # if dataframe is empty, simply return an empty dict
    if dat.shape[0] == 0:
        return {}

    # drop non-numeric columns, if requested
    if skip_non_numeric:
        dat = dat.select_dtypes(include=["float", "int"])

    # quantiles to use
    quants = np.arange(0.05, 1, step=0.05)

    stats = {}

    # overall statistics
    if dat.select_dtypes(include=["float", "int"]).shape[1] == dat.shape[1]:
        stats["overall"] = {
            "mean": float(dat.mean().mean()),
            "std": float(dat.stack().std()),
            "median": float(dat.stack().median()),
            "mad": float(dat.stack().mad()),
            "kurtosis": float(dat.stack().kurtosis()),
            "skew": float(dat.stack().skew()),
            "num_na": int(dat.isna().sum().sum()),
            "num_zero": int((dat == 0).sum().sum()),
            "num_dup": int(dat.duplicated().sum().sum()),
            "min": float(dat.min().min()),
        }

        #  breakpoint()

        for i, quant in enumerate(np.nanquantile(dat, quants)):
            stats["overall"][str(int(100 * quants[i])) + "%"] = float(quant)

        stats["overall"]["max"] = float(dat.max().max())

    # compute row/column statistics
    stats["rows"] = compute_axis_summary(dat, axis=1)
    stats["columns"] = compute_axis_summary(dat, axis=0)

    # compute missing value statistics
    stats["missing_values"] = compute_missing_stats(dat)

    return stats


def compute_axis_summary(dat, axis=0, cor_max_size=5000):
    """Computes basic summary statistics along an axis of a dataframe"""
    import numpy as np

    means = dat.mean(axis=axis)
    meds = dat.median(axis=axis)
    mads = dat.mad(axis=axis)
    stds = dat.std(axis=axis)
    skews = dat.skew(axis=axis)
    kurts = dat.kurtosis(axis=axis)

    stats = {
        "mean_min": float(means.min()),
        "mean_mean": float(means.mean()),
        "mean_max": float(means.max()),
        "median_min": float(meds.min()),
        "median_mean": float(meds.mean()),
        "median_max": float(meds.max()),
        "mad_min": float(mads.min()),
        "mad_mean": float(mads.mean()),
        "mad_max": float(mads.max()),
        "std_min": float(stds.min()),
        "std_mean": float(stds.mean()),
        "std_max": float(stds.max()),
        "skew_min": float(skews.min()),
        "skew_mean": float(skews.mean()),
        "skew_max": float(skews.max()),
        "kurt_min": float(kurts.min()),
        "kurt_mean": float(kurts.mean()),
        "kurt_max": float(kurts.max()),
    }

    # for numeric dataframes, compute correlations along axes that aren't too large
    if dat.select_dtypes(include=["float", "int"]).shape[1] == dat.shape[1] and (
        (axis == 0 and dat.shape[1] <= cor_max_size)
        or (axis == 1 and dat.shape[0] <= cor_max_size)
    ):

        # pearson correlation
        if axis == 0:
            cor_mat = dat.corr()
        else:
            cor_mat = dat.T.corr()

        # get upper triangular matrix, excluding diagonal
        mask = np.triu(np.ones(cor_mat.shape), 1).astype("bool").reshape(cor_mat.size)
        cor_vals = cor_mat.stack(dropna=False)[mask]

        stats["cor_pearson_min"] = float(cor_vals.min())
        stats["cor_pearson_mean"] = float(cor_vals.mean())
        stats["cor_pearson_max"] = float(cor_vals.max())

        # spearman correlation
        if axis == 0:
            cor_mat = dat.corr(method="spearman")
        else:
            cor_mat = dat.T.corr(method="spearman")

        mask = np.triu(np.ones(cor_mat.shape), 1).astype("bool").reshape(cor_mat.size)
        cor_vals = cor_mat.stack(dropna=False)[mask]

        stats["cor_spearman_min"] = float(cor_vals.min())
        stats["cor_spearman_mean"] = float(cor_vals.mean())
        stats["cor_spearman_max"] = float(cor_vals.max())

    return stats


def compute_missing_stats(dat):
    """Computes summary of missing values in the dataset"""
    dat_missing = dat.isnull()

    missing_total = dat_missing.sum().sum()

    missing_dict = {
        "total": {
            "num_missing": int(missing_total),
            "num_missing_pct": float(missing_total / (dat.shape[0] * dat.shape[1])),
        }
    }

    if missing_total > 0:
        missing_dict["rows"] = dat_missing.T.sum().to_dict()
        missing_dict["columns"] = dat_missing.sum().to_dict()

    return missing_dict
