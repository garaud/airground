# coding: utf-8

"""deal with weather data
"""

import requests

from airground import OPEN_WEATHER_APPID
from airground import DARK_SKY_APPID


OPENWEATHER_URL = 'http://api.openweathermap.org/data/2.5'
DARKSKY_URL = 'https://api.darksky.net/forecast'


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


if __name__ == '__main__':
    # close to the Parc Bordelais
    lon, lat = -0.604169377584603, 44.8550083242965
    dks_forcast = darksky_forecast(lat, lon)
    openw_forcast = openweather_forecast(lat, lon)
