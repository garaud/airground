# coding: utf-8

"""Luigi tasks to retrieve data every day
"""

import os
import json
from datetime import date

import requests

import pandas as pd

import luigi
from luigi.format import MixedUnicodeBytes, UTF8

from airground import weather


XLS_PLAYGROUND_DATA = 'http://databordeaux.blob.core.windows.net/data/dref/airejeux.xls'


def read_airground(fpath):
    """Read and preprocess the Airground DataFrame
    """
    df = pd.read_excel(fpath, decimal=',')
    # prefer lower case column names
    df.columns = pd.Index([x.lower() for x in df.columns])
    df = df.rename_axis({"x_long": "lon",
                         "y_lat": "lat"}, axis=1)
    return df


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
    """Download weather forecasts from the OpenWeather API
    """
    date = luigi.DateParameter(default=date.today())
    path = 'data/open-weather-airground-{}.json'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return RawPlaygroundExcelData()

    def run(self):
        df = read_airground(self.input().path)
        forecasts = weather.airground_weather_forecast(df, 'openweather')
        with self.output().open('w') as fobj:
            json.dump(forecasts, fobj)


class DarkskyWeatherJsonAirground(luigi.Task):
    """Download weather forecasts from the DarkSky API.
    """
    date = luigi.DateParameter(default=date.today())
    path = 'data/darksky-weather-airground-{}.json'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return RawPlaygroundExcelData()

    def run(self):
        df = read_airground(self.input().path)
        forecasts = weather.airground_weather_forecast(df, 'darksky')
        with self.output().open('w') as fobj:
            json.dump(forecasts, fobj)


class RawJsonAirground(luigi.WrapperTask):
    """Wrap JSON weather API
    """
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        yield OpenWeatherJsonAirground(self.date)
        yield DarkskyWeatherJsonAirground(self.date)


class FilterOpenWeatherData(luigi.Task):
    """Just get temperature, precipitation data and weather description for each
    available playground.
    """
    date = luigi.DateParameter(default=date.today())
    path = 'data/filtered-openweather-{}.csv'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return {"json": OpenWeatherJsonAirground(self.date),
                "playground": RawPlaygroundExcelData()}

    def run(self):
        with self.input()["json"].open() as fobj:
            data = json.load(fobj)
            result = {key: weather.openweather_datamodel(playground[0])
                      for key, playground in data.items()}
        df = read_airground(self.input()["playground"].path)
        # merge some data
        result = pd.DataFrame(result).T
        result.index = pd.Index(result.index.astype(int))
        with self.output().open("w") as fobj:
            (df[['cle', 'nom', 'age_min', 'age_max']]
             .merge(result, left_on="cle", right_index=True)
             .to_csv(fobj, index=False))


class FilterDarkSkyData(luigi.Task):
    """Just get temperature, precipitation data and weather description for each
    available playground.
    """
    date = luigi.DateParameter(default=date.today())
    path = 'data/filtered-darksky-{}.csv'

    def output(self):
        return luigi.LocalTarget(self.path.format(self.date), format=UTF8)

    def requires(self):
        return {"json": DarkskyWeatherJsonAirground(self.date),
                "playground": RawPlaygroundExcelData()}

    def run(self):
        with self.input()["json"].open() as fobj:
            data = json.load(fobj)
            result = {key: weather.darksky_datamodel(playground[0])
                      for key, playground in data.items()}
        df = read_airground(self.input()["playground"].path)
        # merge some data
        result = pd.DataFrame(result).T
        result.index = pd.Index(result.index.astype(int))
        with self.output().open("w") as fobj:
            (df[['cle', 'nom', 'age_min', 'age_max']]
             .merge(result, left_on="cle", right_index=True)
             .to_csv(fobj, index=False))


class FilterWeatherDataToCSV(luigi.WrapperTask):
    """Wrap filter data tasks
    """
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        yield FilterOpenWeatherData(self.date)
        yield FilterDarkSkyData(self.date)
