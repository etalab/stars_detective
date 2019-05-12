'''Determines the number of datasets that satisfies the Tim Berners Lee (TBL) 5-stars rating system
    (https://5stardata.info/en/)

   It requires two csv files as input: datasets.csv and resources.csv

Usage:
    dgf_stars_detective.py <d> <r> [options]

Arguments:
    <d>                                Datasets csv file path
    <r>                                Resources csv file path
    --num_cores CORES                  Num cores [default:10:int]

'''


import pandas as pd
from argopt import argopt

from utils import check_license, check_online_availability


def one_star(datasets_df, n_cores=10):
    """
    Check that the datasets/resources on datasets_df comply with the first level of the TBL 5-stars system:

    "make your stuff available on the Web (whatever format) under an open license"

    :param datasets_df:
    :param resources_csv:
    :return:
    """

    # 1. Check open licenses
    licenses_info, open_idx = check_license(datasets_df)

    # 2. Check resources are actually on the web
    availability_info, available_idx = check_online_availability(datasets_df, n_cores=n_cores)

    # Intersect both conditions
    open_and_available_idx = open_idx.intersection(available_idx)

    return open_and_available_idx, licenses_info.update(availability_info)


def two_star(datasets_df, resource_df):
    """
    Check that the datasets/resources on datasets_df comply with the second level of the TBL 5-stars system:

    "make it available as structured data (e.g., Excel instead of image scan of a table)"

    :param datasets_df:
    :param resources_csv:
    :return:
    """
    pass


def check_no_resources_datasets(resources_str):
    import datetime
    list_resources = eval(resources_str)
    return len(list_resources)

if __name__ == '__main__':
    parser = argopt(__doc__).parse_args()
    datasets_file_path = parser.d
    resources_folder_path = parser.r
    n_cores = int(parser.num_cores)

    datasets_df = pd.read_csv(datasets_file_path, sep=";").loc[:]
    num_all_datasets = len(datasets_df)

    resources_df = pd.read_csv(resources_folder_path, sep=";")


    one_star_idx, one_star_info = one_star(datasets_df, n_cores=n_cores)


