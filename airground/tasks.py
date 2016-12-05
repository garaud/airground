# coding: utf-8

"""Luigi tasks to retrieve data every day
"""

import os
import json
from datetime import date

import requests

from dateutil.relativedelta import relativedelta

import pandas as pd

import luigi
from luigi.format import MixedUnicodeBytes, UTF8

from airground import weather


XLS_PLAYGROUND_DATA = 'http://databordeaux.blob.core.windows.net/data/dref/airejeux.xls'


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
    priority = 10

    def output(self):
        return luigi.LocalTarget(self.path, format=MixedUnicodeBytes)

    def run(self):
        resp = requests.get(XLS_PLAYGROUND_DATA)
        with self.output().open('w') as fobj:
            fobj.write(resp.content)


class OpenWeatherJsonAirground(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    path = 'data/open-weather-airground-{}.json'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return RawPlaygroundExcelData()

    def run(self):
        df = pd.read_excel(self.input().path, decimal=',')
        # prefer lower case column names
        df.columns = pd.Index([x.lower() for x in df.columns])
        df = df.rename_axis({"x_long": "lon",
                             "y_lat": "lat"}, axis=1)
        forecasts = weather.airground_weather_forecast(df)
        with self.output().open('w') as fobj:
            json.dump(forecasts, fobj)


class DarkskyWeatherJsonAirground(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    path = 'data/darksky-weather-airground-{}.json'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return RawPlaygroundExcelData()

    def run(self):
        df = pd.read_excel(self.input().path, decimal=',')
        # prefer lower case column names
        df.columns = pd.Index([x.lower() for x in df.columns])
        df = df.rename_axis({"x_long": "lon",
                             "y_lat": "lat"}, axis=1)
        forecasts = weather.airground_weather_forecast(df)
        with self.output().open('w') as fobj:
            json.dump(forecasts, fobj)


class JsonAirground(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        yield OpenWeatherJsonAirground(self.date)
        yield DarkskyWeatherJsonAirground(self.date)
