import re

import requests

__author__ = 'Pavel Soriano'
__mail__ = 'sorianopavel@gmail.com'

from collections import Counter
import datetime
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
ProgressBar().register()


def check_license(datasets_df):
    result_dict = {}

    license_dict = {"fr-lo": 'Licence Ouverte / Open Licence',
                    "notspecified": 'License Not Specified',
                    "odc-odbl": 'Open Data Commons Open Database License (ODbL)',
                    "other-open": 'Other (Open)',
                    "cc-zero": 'Creative Commons CCZero',
                    "cc-by": 'Creative Commons Attribution',
                    "cc-by-sa": 'Creative Commons Attribution Share-Alike',
                    "other-at": 'Other (Attribution)',
                    "lov2": 'Licence Ouverte / Open Licence version 2.0',
                    "odc-pddl": 'Open Data Commons Public Domain Dedication and Licence (PDDL)',
                    "other-pd": "Other (Public Domain)",
                    "odc-by": 'Open Data Commons Attribution License'}

    # ['other-pd', 'fr-lo', 'cc-by', 'notspecified', 'cc-by-sa',
    #    'odc-odbl', nan, 'other-open', 'lov2', 'cc-zero', 'other-at',
    #    'odc-pddl', 'odc-by']

    no_open_idx = datasets_df.index[(datasets_df.license.isnull()) | (datasets_df.license.isin(["notspecified"]))]
    open_idx = datasets_df.index[~datasets_df.index.isin(no_open_idx)]

    no_open_df = datasets_df.loc[no_open_idx]
    open_df = datasets_df.loc[open_idx]

    result_dict["num_no_open"] = len(no_open_df)
    result_dict["num_open"] = len(open_df)

    result_dict["pct_no_open"] = 100 * (len(no_open_df) / len(datasets_df))
    result_dict["pct_open"] = 100 - result_dict["pct_no_open"]

    result_dict["no_open_counts"] = Counter(no_open_df.license)
    result_dict["open_counts"] = Counter(open_df.license)

    return result_dict, open_idx


def clean_formats(dataset_series):

    pass


def check_url_works(resources_str):

    resources_lst = eval(resources_str)
    for resource in resources_lst:
        if "url" not in resource:
            continue
        url = resource["url"]
        try:
            r = requests.head(url, allow_redirects=True)
        except:
            continue
        if r.status_code == requests.codes.ok:
            return True
    return False


def check_online_availability(datasets_df):
    result_dict = {}

    no_reseources_idx = datasets_df.index[(datasets_df.resources.isnull()) | (datasets_df.resources.isin(["[]"]))]
    with_resources_idx = datasets_df.index[~datasets_df.index.isin(no_reseources_idx)]

    result_dict["no_resources"] = len(no_reseources_idx)
    result_dict["with_resources"] = len(with_resources_idx)

    result_dict["pct_no_resources"] = 100 * (len(no_reseources_idx) / len(datasets_df))
    result_dict["pct_with_resources"] = 100 - result_dict["pct_no_resources"]

    with_resources_df = datasets_df.loc[with_resources_idx]

    with_resources_dd = dd.from_pandas(with_resources_df, npartitions=3)

    res = with_resources_dd.map_partitions(lambda df: df.apply(lambda x: check_url_works(x.resources), axis=1), meta=("result", bool)).compute(scheduler="processes")
    with_resources_df["available"] = res
    del with_resources_dd

    available_idx = with_resources_df.index[with_resources_df.available is True]

    result_dict["available"] = len(available_idx)
    result_dict["not_available"] = len(datasets_df) - len(available_idx)
    result_dict["pct_available"] = 100 * (len(available_idx) / len(datasets_df))
    result_dict["pct_not_available"] = 100 - result_dict["pct_available"]

    return result_dict, available_idx

extension_re = re.compile(r'/[\w_\-]+\.(\w+)$')


def try_to_get_format(dataset):
    dataset = dataset.fillna("")
    if dataset.format != "":
        return dataset.format
    # Try to get format from url
    if dataset.url:
        print("Trying regex")
        matcho = extension_re.findall(dataset.url)
        if matcho:
            return matcho[-1]
    # Go to the url and try to get it from the header
        try:
            print("Trying download")
            r = requests.head(dataset.url, allow_redirects=True)
            extension = r.headers["Content-Type"].split("/")[-1]
            return extension
        except:
            return ""
    return ""


def try_toget_format_wrap(resources_df:pd.DataFrame, n_cores=5):
    resources_dd = dd.from_pandas(resources_df, npartitions=n_cores)

    res = resources_dd.map_partitions(lambda df: df.apply(lambda x: try_to_get_format(x), axis=1), meta=("result", str)).compute(scheduler="processes")
    resources_df["maybe_format"] = res
    resources_df.to_csv("input_files/resources_maybe_formats.csv", sep=";")
