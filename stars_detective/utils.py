import re

import requests

from stars_detective import logger

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


def check_url_works(resources_series:pd.Series):
    resources_lst = eval(resources_series)

    for resource in resources_lst:
        if "extras" in resource:
            if "check:available" in resource["extras"]:
                if resource["extras"]["check:available"]:
                    return True
        if "url" not in resource:
            continue
        url = resource["url"]
        try:
            r = requests.head(url, allow_redirects=True)
            if r.status_code == requests.codes.ok:
                return True
        except Exception as e:
            logger.debug("Failed checking {}".format(url))
            logger.error(e)
            continue
    return False


def check_online_availability(datasets_df, n_cores=10):
    result_dict = {}

    no_resources_idx = datasets_df.index[(datasets_df.resources.isnull()) | (datasets_df.resources.isin(["[]"]))]
    with_resources_idx = datasets_df.index[~datasets_df.index.isin(no_resources_idx)]

    result_dict["no_resources"] = len(no_resources_idx)
    result_dict["with_resources"] = len(with_resources_idx)

    result_dict["pct_no_resources"] = 100 * (len(no_resources_idx) / len(datasets_df))
    result_dict["pct_with_resources"] = 100 - result_dict["pct_no_resources"]

    with_resources_df = datasets_df.loc[with_resources_idx]

    with_resources_dd = dd.from_pandas(with_resources_df, npartitions=n_cores)

    res = with_resources_dd.map_partitions(lambda df: df.apply(lambda x: check_url_works(x.resources), axis=1),
                                           meta=("result", bool)).compute(scheduler="processes")
    with_resources_df["available"] = res
    # del with_resources_dd

    available_idx = with_resources_df.index[with_resources_df.available == True]

    result_dict["available"] = len(available_idx)
    result_dict["not_available"] = len(datasets_df) - len(available_idx)
    result_dict["pct_available"] = 100 * (len(available_idx) / len(datasets_df))
    result_dict["pct_not_available"] = 100 - result_dict["pct_available"]

    return result_dict, available_idx




