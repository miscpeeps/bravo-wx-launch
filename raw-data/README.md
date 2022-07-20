Folder for raw data from KSC Weather sources

# Folder Structure
Please create subfolders in the following construct: YYYYMMDD-scrub or YYYYMMDD-launch

# Data sources
We are pulling 4 hours of data previous to the time of each event (launch or scrub).

For each 4 hour time block, place the .csv file in a folder corresponding to the folder structure above

Each folder should have 6 .csv files, 1 file exported per each of the following sites:
* [915 Mhz Doppler Radar Wind Profiler Filters](https://kscweather.ksc.nasa.gov/wxarchive/WindProfiler915)
* [48 Mhz Tropospheric Doppler Radar Wind Profiler Filters](https://kscweather.ksc.nasa.gov/wxarchive/WindProfiler50)
* [AMPS Low Resolution Filters](https://kscweather.ksc.nasa.gov/wxarchive/AmpsLowResolution)
* [Merlin Cloud to Ground Lightning System (MERLIN C-G) Filters](https://kscweather.ksc.nasa.gov/wxarchive/MerlinCloudToGround)
* [Weather Tower Filters -- leave default instrument sites (19/69 selected)](https://kscweather.ksc.nasa.gov/wxarchive/WeatherTower)
* [Rainfall Filters](https://kscweather.ksc.nasa.gov/wxarchive/Rainfall)

If a time period does not have a data set for the time period place a file in the folder for its time period per the following naming convention:
* no_915mhz_data_for_time_period.md
* no_50mhz_data_for_time_period.md
* no_ampslow_data_for_time_period.md
* no_merlincg_data_for_time_period.md
* no_tower_data_for_time_period.md
* no_rain_data_for_time_period.md
