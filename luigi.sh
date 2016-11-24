#!/bin/bash

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$here

cd $here/airground
workers=4

luigi --workers $workers --parallel-scheduling --local-scheduler --module airground.tasks RawPlaygroundExcelData
