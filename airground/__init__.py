# coding: utf-8

"""Airground package
"""

import os
import logging

# API Token
OPEN_WEATHER_APPID = os.environ.get("OPEN_WEATHER_APPID", "WRONG_KEY")
DARK_SKY_APPID = os.environ.get("DARK_SKY_APPID", "WRONG_KEY")

LOG_FORMAT = "%(levelname)s :: %(asctime)s :: %(module)s :: %(funcName)s :: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
