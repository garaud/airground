# coding: utf-8

"""Luigi tasks to retrieve data every day
"""


import requests

import luigi
from luigi.format import UTF8, MixedUnicodeBytes


XLS_PLAYGROUND_DATA = 'http://databordeaux.blob.core.windows.net/data/dref/airejeux.xls'


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
