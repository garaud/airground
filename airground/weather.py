# coding: utf-8

"""deal with weather data
"""

import logging
from collections import defaultdict
from datetime import datetime

import requests

import pandas as pd

from airground import OPEN_WEATHER_APPID
from airground import DARK_SKY_APPID


OPENWEATHER_URL = 'http://api.openweathermap.org/data/2.5'
DARKSKY_URL = 'https://api.darksky.net/forecast'
SUPPORTED_SITE = ('openweather', 'darksky')

Logger = logging.getLogger(__name__)


def darksky_forecast(lat, lon):
    """Weather forecast from the Dark Sky website

    lat: float
        Longitude coordinate
    lon: float
        Latitude coordinate

    Return a forecast for the given coordinate
    """
    url = "/".join([DARKSKY_URL, DARK_SKY_APPID,
                    ",".join([str(lat), str(lon)])])
    query_params = {"units": "si",
                    "exclude": ["minutely", "alerts"]}
    return requests.get(url, query_params).json()


def openweather_forecast(lat, lon):
    """Weather forecast from the Open Weather website

    lat: float
        Longitude coordinate
    lon: float
        Latitude coordinate

    Return a forecast for the given coordinate
    """
    url = "/".join([OPENWEATHER_URL, 'forecast'])
    query_params = {"units": "metric",
                    "appid": OPEN_WEATHER_APPID,
                    "lon": lon,
                    "lat": lat}
    return requests.get(url, query_params).json()


def current_weather(lat, lon):
    """Get the current weather for a specific coodinates

    lat: float
        Longitude coordinate
    lon: float
        Latitude coordinate

    Return the current weather
    """
    url = "/".join([OPENWEATHER_URL, 'weather'])
    query_params = {"units": "metric",
                    "appid": OPEN_WEATHER_APPID,
                    "lon": lon,
                    "lat": lat}
    return requests.get(url, query_params).json()


def darksky_datamodel(data):
    """Extract some fields from the Darksky forecasts

    https://api.darksky.net/forecast
    """
    # next hour
    weather = data['hourly']['data'][1]
    return {"temperature": weather['temperature'],
            "at": datetime.fromtimestamp(weather['time']),
            "desc": weather['summary'],
            "rain": weather['precipIntensity']}


def openweather_datamodel(data):
    """Extract some fields from the OpenWeather forecasts

    http://api.openweathermap.org/data/2.5/forecast
    """
    # next 3-hour
    weather = data['list'][1]
    return {"temperature": weather['main']['temp'],
            "at": datetime.fromtimestamp(weather['dt']),
            "desc": weather['weather'][0]['main'],
            "rain": weather['rain'].get("3h", 0)}


def airground_weather_forecast(data, site_type='openweather'):
    """Give the weather forecast for all playground

    data: DataFrame
    site_type: str
       Site to get weather data

    Return DataFrame
    """
    Logger.info("request weather for %s airground places", data.shape[0])
    Logger.info("Weather data come from '%s'", site_type)
    result = defaultdict(list)
    if site_type not in SUPPORTED_SITE:
        raise ValueError("site_type is '{}'. It must be in {}".format(site_type, SUPPORTED_SITE))
    if site_type == 'openweather':
        forecast = openweather_forecast
        model = openweather_datamodel
    if site_type == 'darksky':
        forecast = darksky_forecast
        model = darksky_datamodel
    for _, airground in data.iterrows():
        lat, lon = airground[['lat', 'lon']]
        wair = model(forecast(lat, lon))
        for key in wair:
            result[key].append(wair[key])
        result['id'].append(airground['cle'])
    return pd.DataFrame(result)


if __name__ == '__main__':
    # close to the Parc Bordelais
    lon, lat = -0.604169377584603, 44.8550083242965
    dks_forcast = darksky_forecast(lat, lon)
    openw_forcast = openweather_forecast(lat, lon)
    df = pd.read_excel('data/airejeux.xls', decimal=',')
    # prefer lower case column names
    df.columns = pd.Index([x.lower() for x in df.columns])
    df = df.rename_axis({"x_long": "lon",
                         "y_lat": "lat"}, axis=1)
    top5 = df.head()
    # wf = airground_weather_forecast(top5)
