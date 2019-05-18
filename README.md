# stars_detective
Script to determine the proportion of datasets that comply with the [Tim Berners Lee 5-stars](https://5stardata.info/en/) system in the open data portal *data.gouv.fr* (dgf). In `output_files/` you can find a csv file containing the `dataset_id` and the number of stars for each dataset.


# How it is done ?
We have two files where we the details of datasets and their corresponding resources are stored (`datasets.csv` and `resources.csv`, respectively). There are **39013** datasets in total. There are **199818** resources. A dataset can have more than one resource.

## Level *
>"make your stuff available on the Web (whatever format) under an open license"

We check that the datasets are assigned with an open licence and that they are available on the internet. 

Among the existing licences in the datasets stored in dgf (see below) we check how many are `notspecified` or `nan`.
```python
{"fr-lo": 'Licence Ouverte / Open Licence',
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
```

## Level **

>"make it available as structured data (e.g., Excel instead of image scan of a table)"

From the available top 20 formats (these formats cover around 97% of the total number of **resources**, their frequency is the second value of each tuple):

```python
[('json', 41919), ('shp', 32149), ('csv', 27789), ('zip', 26069), ('pdf', 24123), ('xml', 11943), ('html', 10740),
('xls', 5580), ('image', 3437), ('ods', 2007), ('xlsx', 1713), ('.asc, .las, .glz', 1048), ('geojson', 787),
('kml', 764), ('bin', 760), ('kmz', 543), ('txt', 498), ('doc', 456), ('api', 391), ('dbf', 352)]
```

`pdf`, `image`, `bin`, and `doc` are not structured
 
 ## Level ***
 
 >"make it available in a non-proprietary open format (e.g., CSV instead of Excel)"
 
 Again, among the formats above, filter `xls` and `dbf` as they are the extensions that are non-proprietary
 
 ## Level ****

>"use URIs to denote things, so that people can point at your stuff"

```python
["ttl", "owx", "owl", "rdf", "nq", "nt", "trig", "jsonld", "trdf", "rt", "rj", "trix"]
```
We look for these kind of files. We do not consider `xml` nor `html` files as empirically we know that the proportion of true semantic documents within these formats is minimal (within our db).

## Level *****

>"use URIs to denote things, so that people can point at your stuff"

Among those semantic files in the level \*\*\*, we look within them for URIs that reference external entities.
This one is harder because of the multiple types of semantic docs there are. 


## Results

| Level        | pct (\*100)  |
|------------------|----------|
| One star pct:    | 0.75     |
| Two stars pct:   | 0.73     |
| Three stars pct: | 0.71     |
| Four stars pct:  | 0.00033  |
| Five stars pct:  | 0.00012  |

Luckily, we only need to give a broad range, so for the first three levels, we are 71-80%. For the last two, we are 0-9% ¯\\_(ツ)_/¯.
 



