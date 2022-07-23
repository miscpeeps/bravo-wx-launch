# bravo-wx-launch
Repo for weather data and ML model development for the BRAVO hackathon

## How to use
* clone repo
* `$ pip install -r requirements.txt`
* transform the raw data sets `$ python raw-data-transform-multi.py`
* pipe data to models
* train models

# Good Ideas for the Future
* complete and fix sensor data set
* fix logging for multithreading data processing
* implement kwargs for launching data processing per desired conditions
* modify data processing code to accomodate new data sources
* train and evaluate new types of models
