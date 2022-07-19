Folder for raw data from KSC Weather sources

# Folder Structure
Please create subfolders in the following construct: YYYYMMDD-scrub or YYYMMDD-launch

# Data sources
We are pulling 4 hours of data previous to the time of each event (launch or scrub).

For each 4 hour time block, place the .csv file in a folder corresponding to the folder structure above

Each folder should have 5 .csv files, 1 file exported per each of the following sites:
* [915 Mhz Doppler Radar Wind Profiler Filters](https://kscweather.ksc.nasa.gov/wxarchive/WindProfiler915)
* [48 Mhz Tropospheric Doppler Radar Wind Profiler Filters](https://kscweather.ksc.nasa.gov/wxarchive/WindProfiler50)
* [AMPS Low Resolution Filters](https://kscweather.ksc.nasa.gov/wxarchive/AmpsLowResolution)
* [Merlin Cloud to Ground Lightning System (MERLIN C-G) Filters](https://kscweather.ksc.nasa.gov/wxarchive/MerlinCloudToGround)
* [Weather Tower Filters -- leave default instrument sites (19/69 selected)](https://kscweather.ksc.nasa.gov/wxarchive/WeatherTower)
