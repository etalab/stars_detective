'''Determines the number of datasets that satisfies the Tim Berners Lee (TBL) 5-stars rating system
    (https://5stardata.info/en/)

   It requires two csv files as input: datasets.csv and resources.csv
   It is recommended to use the resources_maybe_formats.csv file as it has solved some issues with the formats
   of the resources, such as:
   1. Tried to infer the resource format from the url for nan's and for "document" format types.
   2. Tried to infer the resource format from the files within the zips ("zip" format)


Usage:
    dgf_stars_detective.py <d> <r> [options]

Arguments:
    <d>                                Datasets csv file path
    <r>                                Resources csv file path
    --num_cores CORES                  Num cores [default:10:int]

'''
from collections import Counter

import pandas as pd
import rdflib
from argopt import argopt
import datetime

from stars_detective import logger
from stars_detective.utils import check_license, check_online_availability, check_structured, check_non_proprietary, \
    check_semantic, check_semantic_context


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
    open_and_available_idx: pd.Series = open_idx.intersection(available_idx)

    # Save found indices
    open_and_available_idx.to_series().to_csv("output_files/one_star_indices.csv", header=False, index=False)
    licenses_info.update(availability_info)
    return open_and_available_idx, licenses_info


def two_stars(datasets_df, resources_df, n_cores=20):
    """
    Check that the datasets/resources on datasets_df comply with the second level of the TBL 5-stars system:

    "make it available as structured data (e.g., Excel instead of image scan of a table)"

    From the available top 20 formats:
    [('json', 41919), ('shp', 32149), ('csv', 27789), ('zip', 26069), ('pdf', 24123), ('xml', 11943), ('html', 10740),
    ('xls', 5580), ('image', 3437), ('ods', 2007), ('xlsx', 1713), ('.asc, .las, .glz', 1048), ('geojson', 787),
    ('kml', 764), ('bin', 760), ('kmz', 543), ('txt', 498), ('doc', 456), ('api', 391), ('dbf', 352)]

    pdf, image, bin are not structured (machine-readable, copy paste info from one app to another)

    :param datasets_df:
    :param resources_csv:
    :return:
    """
    results_dict = {}
    results_dict["format_distribution"] = list(sorted(Counter(resources_df.format).items(), key=lambda x: x[1],
                                                      reverse=True)[:20])

    non_machine_readable = ["pdf", "image", "bin", "doc"]

    info_dict, structured_idx = check_structured(datasets_df, resources_df, non_machine_readable, n_cores=n_cores)

    structured_idx.to_series().to_csv("output_files/two_star_indices.csv", header=True, index=False)
    results_dict.update(info_dict)
    return structured_idx, results_dict


def three_stars(datasets_df, resources_df, n_cores=20):
    """
    Check that the datasets/resources on datasets_df comply with the third level of the TBL 5-stars system:

    "make it available in a non-proprietary open format (e.g., CSV instead of Excel)"

    From the available top 20 formats (they cover over 96% of the total number of resources):
    [('json', 41919), ('shp', 32149), ('csv', 27789), ('zip', 26069), ('pdf', 24123), ('xml', 11943), ('html', 10740),
    ('xls', 5580), ('image', 3437), ('ods', 2007), ('xlsx', 1713), ('.asc, .las, .glz', 1048), ('geojson', 787),
    ('kml', 764), ('bin', 760), ('kmz', 543), ('txt', 498), ('doc', 456), ('api', 391), ('dbf', 352)]

    xls, dbf are not structured (machine-readable, copy paste info from one app to another)

    :param datasets_df:
    :param resources_csv:
    :return:
    """

    proprietary_formats = ["xls", "dbf"]

    results_dict, non_proprietary_idx = check_non_proprietary(datasets_df, resources_df, proprietary_formats,
                                                              n_cores=n_cores)
    non_proprietary_idx.to_series().to_csv("output_files/three_star_indices.csv", header=True, index=False)

    return non_proprietary_idx, results_dict


def four_stars(datasets_df, resources_df, n_cores=20):
    """
    Check that the datasets/resources on datasets_df comply with the fourth level of the TBL 5-stars system:

    "use URIs to denote things, so that people can point at your stuff4"

    Considering the formats ttl, owx, owl, and rdf default 'semantic stuff'
    We need to check if the xml's, json's, and maybe the html's contain rdf stuff ??

    :param datasets_df:
    :param resources_csv:
    :return:
    """

    semantic_formats = ["ttl", "owx", "owl", "rdf", "nq", "nt", "trig", "jsonld", "trdf", "rt", "rj", "trix"]
    # semantic_formats = ["ttl", "owx", "owl"]

    results_dict, semantic_idx = check_semantic(datasets_df, resources_df, semantic_formats, n_cores=n_cores)

    semantic_idx.to_series().to_csv("output_files/four_star_indices.csv", header=True, index=False)

    return semantic_idx, results_dict


def five_stars(datasets_df, resources_df, n_cores=20):
    """
    Check that the datasets/resources on datasets_df comply with the fifth level of the TBL 5-stars system:

    "use URIs to denote things, so that people can point at your stuff"

    We check the namespaces of the semantic files (see four_stars) and determine if they link or not to other things

    :param datasets_df:
    :param resources_csv:
    :param n_cores
    :return:
    """
    five_stars_idx = check_semantic_context(datasets_df, resources_df, n_cores=n_cores)
    return five_stars_idx

if __name__ == '__main__':
    parser = argopt(__doc__).parse_args()
    datasets_file_path = parser.d
    resources_folder_path = parser.r
    n_cores = int(parser.num_cores)

    datasets_df = pd.read_csv(datasets_file_path, sep=";").loc[:]
    num_all_datasets = len(datasets_df)

    resources_df = pd.read_csv(resources_folder_path, sep=";")

    one_star_idx, one_star_info = one_star(datasets_df, n_cores=n_cores)

    two_stars_idx, two_star_info = two_stars(datasets_df.loc[one_star_idx], resources_df, n_cores=n_cores)

    three_stars_idx, three_star_info = three_stars(datasets_df.loc[two_stars_idx], resources_df, n_cores=n_cores)

    four_stars_idx, four_star_info = four_stars(datasets_df.loc[three_stars_idx], resources_df, n_cores=n_cores)

    # four_stars_idx = pd.read_csv("output_files/four_star_indices.csv", header=None)
    # four_stars_idx.index = four_stars_idx[0].values

    five_stars_idx = five_stars(datasets_df.loc[four_stars_idx], resources_df, n_cores=n_cores)

    print("One star pct:", len(one_star_idx) / num_all_datasets, str(one_star_info))
    print("Two stars pct:", len(two_stars_idx) / num_all_datasets)
    print("Three stars pct:", len(three_stars_idx) / num_all_datasets)
    print("Four stars pct:", len(four_stars_idx) / num_all_datasets)
    print("Four stars pct:", len(five_stars_idx) / num_all_datasets)