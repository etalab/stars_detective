from collections import Counter

import dask.dataframe as dd
import pandas as pd
import requests
from dask.diagnostics import ProgressBar

from stars_detective import logger
import datetime
# from dask.distributed import Client
# client = Client()


ProgressBar().register()
import numpy as np

RESOURCES_DF = None


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


def check_format(resources_series, list_formats, list_is_negative=True):
    if not isinstance(resources_series, str):
        return False
    resources_lst = eval(resources_series)
    for resource in resources_lst:
        try:
            if "_id" not in resource:
                continue
            res_id = resource["_id"]
            if list_is_negative:
                if RESOURCES_DF.loc[RESOURCES_DF["_id"] == res_id, "format"].iloc[0] not in list_formats:
                    return True
                else:
                    continue
            else:  # list is positive (we are looking for formats within list_formats)
                if RESOURCES_DF.loc[RESOURCES_DF["_id"] == res_id, "format"].iloc[0] in list_formats:
                    return True
        except Exception as e:
            logger.error(resources_series)
            logger.error(e)
            continue
    return False


def check_non_proprietary(datasets_df, resources_df, non_desired_formats, n_cores=10):
    global RESOURCES_DF
    RESOURCES_DF = resources_df

    datasets_dd = dd.from_pandas(datasets_df, npartitions=n_cores)

    res = datasets_dd.map_partitions(
        lambda df: df.apply(lambda x: check_format(x.resources, non_desired_formats), axis=1),
        meta=("result", bool)).compute(scheduler="multiprocessing")
    non_proprietary_idx = res.index[res == True]
    return {"num_non_proprietary": len(non_proprietary_idx)}, non_proprietary_idx


def check_structured(datasets_df, resources_df, non_desired_formats, n_cores=10):
    global RESOURCES_DF
    RESOURCES_DF = resources_df

    datasets_dd = dd.from_pandas(datasets_df, npartitions=n_cores)

    res = datasets_dd.map_partitions(
        lambda df: df.apply(lambda x: check_format(x.resources, non_desired_formats), axis=1),
        meta=("result", bool)).compute(scheduler="multiprocessing")
    structured_idx = res.index[res == True]
    return {"num_structured": len(structured_idx)}, structured_idx


def check_semantic(datasets_df, resources_df, semantic_formats, n_cores=20):
    global RESOURCES_DF
    RESOURCES_DF = resources_df

    datasets_dd = dd.from_pandas(datasets_df, npartitions=n_cores)

    res = datasets_dd.map_partitions(lambda df: df.apply(lambda x: check_format(x.resources, semantic_formats,
                                                                                list_is_negative=False), axis=1),
                                     meta=("result", bool)).compute(scheduler="multiprocessing")
    semantic_idx = res.index[res == True]
    return {"num_semantic": len(semantic_idx)}, semantic_idx


def check_url_works(resources_series: pd.Series):
    resources_lst = eval(resources_series)

    for resource in resources_lst:
        if "extras" in resource:
            if "check:available" in resource["extras"]:
                if resource["extras"]["check:available"]:
                    logger.info("Dataset {0} already checked as available".format(resource["_id"]))
                    return True
                else:
                    logger.info("Dataset {0} already checked as not available".format(resource["_id"]))
                    return False

        if "url" not in resource:
            continue
        url = resource["url"]
        try:
            r = requests.head(url, allow_redirects=True, timeout=5)
            if r.status_code == requests.codes.ok:
                return True
        except Exception as e:
            logger.info("Failed checking {0}, id {1}".format(url, resource["_id"]))
            # logger.error(e)
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
                                           meta=("result", bool)).compute(scheduler="multiprocessing")
    with_resources_df["available"] = res
    # del with_resources_dd

    available_idx = with_resources_df.index[with_resources_df.available == True]

    result_dict["available"] = len(available_idx)
    result_dict["not_available"] = len(datasets_df) - len(available_idx)
    result_dict["pct_available"] = 100 * (len(available_idx) / len(datasets_df))
    result_dict["pct_not_available"] = 100 - result_dict["pct_available"]

    return result_dict, available_idx
