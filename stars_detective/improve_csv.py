from collections import Counter
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import sys

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import requests
import re

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

ProgressBar().register()
extension_re = re.compile(r'/[\w_\-]+\.(\w+)$')

def get_format_for_NAs(dataset):
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


def get_format_for_zips(dataset, popular_formats):
    # Try to download zip from the interwebs
    if dataset.maybe_format != "zip":
        return dataset.maybe_format
    if dataset.url:
        try:
            logger.debug("Downloading {}".format(dataset.url))
            resp = requests.get(dataset.url, allow_redirects=True)
            zipfile = ZipFile(BytesIO(resp.content))
            zip_formats = [f.split(".")[-1] for f in zipfile.namelist()]
            chosen_format = zip_formats[0]
            for f in zip_formats:
                if f in popular_formats:
                    chosen_format = f
                    break
            return chosen_format
        except Exception as e:
            logger.debug("Failed downloading {}".format(dataset.url))
            logger.error(e)
            return "zip"

def get_format_from_url(resources_df:pd.DataFrame, n_cores=5):
    resources_dd = dd.from_pandas(resources_df, npartitions=n_cores)

    res = resources_dd.map_partitions(lambda df: df.apply(lambda x: get_format_for_NAs(x), axis=1), meta=("result", str)).compute(scheduler="processes")
    resources_df["maybe_format"] = res
    resources_df.to_csv("input_files/resources_maybe_formats.csv", sep=";")
    return resources_df


def get_format_from_zip(resources_df:pd.DataFrame, n_sample=None, n_cores=5):
    popular_formats = list(zip(*sorted(Counter(resources_df.maybe_format).items(), key=lambda  x: x[1], reverse=True)[:20]))[0]

    if n_sample:
        resources_df = resources_df.sample(n_sample, random_state=42)

    resources_dd = dd.from_pandas(resources_df, npartitions=n_cores)
    res = resources_dd.map_partitions(lambda df: df.apply(lambda x: get_format_for_zips(x, popular_formats), axis=1),
                                      meta=("result", str)).compute(scheduler="processes")
    resources_df["maybe_format"] = res
    return resources_df


if __name__ == '__main__':
    n_cores = int(sys.argv[1])

    # resources_df = pd.read_csv("input_files/resources.csv", sep=";")

    # Try to get missing format (resources with nan in format) from url

    # resources_df = get_format_from_url(resources_df, n_cores=n_cores)
    # resources_df.to_csv("input_files/resources_maybe_formats.csv", sep=";")



    resources_df = pd.read_csv("input_files/resources_maybe_formats.csv", sep=";")
    # Replace detected  'html;charset=UTF-8' and 'html; charset=utf-8' by 'html'
    resources_df.loc[resources_df.maybe_format.isin(["html;charset=UTF-8", "html; charset=utf-8"]), "maybe_format"] = "html"

    # Replace detected  'xml;charset=UTF-8' by 'xml'
    resources_df.loc[resources_df.maybe_format.isin(["xml;charset=UTF-8"]), "maybe_format"] = "xml"

    # Replace detected  'json;odata=verbose;charset=utf-8' by 'json'
    resources_df.loc[resources_df.maybe_format.isin(["json;odata=verbose;charset=utf-8"]), "maybe_format"] = "json"

    # Try to get a specific format for "document" type format, also from the url
    resources_df.loc[resources_df.maybe_format == "document", "maybe_format"] = resources_df.loc[resources_df.maybe_format == "document", "url"].apply(lambda x: x[-3:])

    # Try to get the format for the zip files
    resources_df = get_format_from_zip(resources_df, n_cores=n_cores, n_sample=200)
    # resources_df.to_csv("input_files/resources_maybe_formats.csv", sep=";")
    pass
