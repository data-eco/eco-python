"""
Utility functions for data packaging pipeline
KH (Oct 2, 2020)
"""
import numpy as np
import pandas as pd


def load_tbl(infile):
    """Loads a tabular data file from disk"""
    if infile.endswith(".csv") or infile.endswith(".csv.gz"):
        dat = pd.read_csv(infile)
    elif infile.endswith(".tsv") or infile.endswith(".tsv.gz"):
        dat = pd.read_csv(infile, sep="\t")
    elif infile.endswith(".txt") or infile.endswith(".txt.gz"):
        dat = pd.read_csv(infile, sep="\t")
    elif infile.endswith(".parquet"):
        dat = pd.read_parquet(infile)
    elif infile.endswith(".feather"):
        dat = pd.read_feather(infile)
    else:
        print("Unrecognized file format!")
        print(infile)

    return dat
