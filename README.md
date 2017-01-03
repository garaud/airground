# Airground

Check the weather and air quality in the Bordeaux (FR) playgrounds.

This is an example how you can use [luigi](http://github.com/spotify/luigi), a
Python module that helps you build complex pipelines of batch jobs.

Weather and air quality data are available from some API Websites. You **need** a
token to request some data.

See the bash script `luigi.sh` to launch the data processing with luigi. Read
the [official documentation](http://luigi.readthedocs.io/en/stable) to see how
you can launch
[the central scheduler](http://luigi.readthedocs.io/en/stable/central_scheduler.html).

## Weather Data

* Dark Sky API
* OpenWeather API

## Air Quality Data

* Plume API

## Requirements

* python 3
* luigi
* pandas
* requests
* xlrd

`conda_env.sh` can create a conda environment dedicated to this package.
Activate it with `source activate airground`.
