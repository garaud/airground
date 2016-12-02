#!/bin/bash

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$here

cd $here/airground
workers=3

# luigi --workers $workers --parallel-scheduling --local-scheduler --module airground.tasks RawPlaygroundExcelData
luigi --workers $workers --parallel-scheduling --local-scheduler --module airground.tasks JsonAirground
