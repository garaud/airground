# coding: utf-8

"""Luigi tasks to retrieve data every day
"""

import os
from datetime import date

import requests

from dateutil.relativedelta import relativedelta

import pandas as pd

import luigi
from luigi.format import MixedUnicodeBytes, UTF8


XLS_PLAYGROUND_DATA = 'http://databordeaux.blob.core.windows.net/data/dref/airejeux.xls'
OPEN_WEATHER_APPID = os.environ.get("OPEN_WEATHER_APPID", "WRONG_KEY")


def yesterday():
    return date.today() - relativedelta(days=1)


def bounding_box(coordinates):
    """Return the bounding box from a list of coordinates

    (lon,lat) top left to the bottom right

    coordinates: DataFrame
        (lon,lat)

    Return top,bottom right/left point
    """
    return {"lon_top_left": coordinates["lon"].min(),
            "lat_top_left": coordinates["lon"].max(),
            "lon_bottom_right": coordinates["lat"].max(),
            "lat_bottom_right": coordinates["lat"].min()}


class RawPlaygroundExcelData(luigi.Task):
    """Download data about playground locations and types.
    """
    path = 'data/airejeux.xls'

    def output(self):
        return luigi.LocalTarget(self.path, format=MixedUnicodeBytes)

    def run(self):
        resp = requests.get(XLS_PLAYGROUND_DATA)
        with self.output().open('w') as fobj:
            fobj.write(resp.content)


class WeatherStations(luigi.Task):
    date = luigi.DateParameter(default=yesterday())
    path = 'data/weather-stations-{}.json'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return RawPlaygroundExcelData()

    def run(self):
        df = pd.read_excel(self.input().path, decimal=',')
        # prefer lower case column name
        df.columns = pd.Index([x.lower() for x in df.columns])
        df = df.rename_axis({"x_long": "lon",
                             "y_lat": "lat"}, axis=1)
        bbox = bounding_box(df)
        zoom = 6
        bbox_url = ",".join("{:2.2f}".format(x) for x in
                            [bbox['lon_top_left'], bbox['lat_top_left'],
                             bbox['lon_bottom_right'], bbox['lat_bottom_right']])
        # for the zoom
        bbox_url += ",6"
