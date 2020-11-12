from urllib import request
from zipfile import ZipFile
from io import BytesIO
import csv
from operator import itemgetter
import itertools as it
import json
import os


def function_points_quality(grupo):
    points = []
    quality = []
    for row in grupo:
        points.append([row['TIME_PERIOD'] + ' , ' + row['OBS_VALUE']])
        quality.append([row['TIME_PERIOD'] + ' , ' + row['ASSESSMENT_CODE']])

    return points, quality


response = request.urlopen(
    "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip")
if os.path.exists("downloads") != True:
    os.mkdir("downloads")

unzip = ZipFile(BytesIO(response.read()))
unzip.extractall("downloads")
unzip.close()

with open("downloads/jodi_gas_beta.csv", 'r') as mycsv:

    reader = csv.DictReader(mycsv)

    primary_key = itemgetter('REF_AREA', 'ENERGY_PRODUCT', 'FLOW_BREAKDOWN', 'UNIT_MEASURE',
                             'TIME_PERIOD')
    reader = sorted(reader, key=primary_key)


keys, groups = [], []
keyfunc = itemgetter('REF_AREA', 'ENERGY_PRODUCT',
                     'FLOW_BREAKDOWN', 'UNIT_MEASURE')
for k, g in it.groupby(reader, key=keyfunc):
    keys.append(k[0]+k[1]+k[2]+k[3])
    groups.append(list(g))


for i, group in enumerate(groups):

    points, quality = function_points_quality(group)
    resultado = {
        'series_id': ''.join(keys[i]),
        'points': points,
        'series': {
            'country': ''.join(group[0]['REF_AREA']),
            'product': ''.join(group[0]['ENERGY_PRODUCT']),
            'flow': ''.join(group[0]['FLOW_BREAKDOWN']),
                    'unit_meassure': ''.join(group[0]['UNIT_MEASURE']),
                    'quality': quality
        }
    }

    result = json.dumps(resultado)

    print(result)
