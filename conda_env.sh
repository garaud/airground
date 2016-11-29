#!/bin/bash

echo "Create the conda environment 'airground'"

deps="pandas requests luigi xlrd"

conda create -n airground $deps

echo "environment created with the dependencies '$deps'"
