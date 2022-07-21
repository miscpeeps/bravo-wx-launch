# Data Transforms

## Overall 

1. 5 Minute timesteps for the whole of the dataset
2. Generally groupby/bin time for sensors with less resolution and then consider how to effectively transform data
3. Rows will be columns in final data sets (in general) so data transformations to condense information are important and must be done surgically
4. Normalization is currently assumed to be done with fit() functions in sklearn, if not further considerations are needed for how to normalize

## Amps-low
1. Consider case of missing balloons
2. For all row/columns, bin values into altitude groups [16, 1000-10000,10000-25000,25000+] and interpolate time based on normal rise rate
3. For wind speed, interpolate wind speed same as time and ignore altitude
4. average PrecipitableWater
5. discard all else 

## Wind-Profiler 915 mhz
1. Groupby sensor
2. bin by altitude, [.2-.8, .8-1.5, 1.5-2.1]
3. take max wind speed from each bin
4. take variance of wind direction (may have to have do coordinate grid shenanigans) (( considering ))

## Wind-Profiler 50 Mhz
1. bin by altitude [0-5000, 5000-8000, 8000-11000, 11000-14000,14000-17000,17000+]
2. take max wind speed from each bin
3. take variance of wind direction (may have to have do coordinate grid shenanigans) (( considering ))
4. take max wind shear
5. Keep WW, may get PCA'd

## Weather-Tower
1. Groupby tower
3. fill NAs with average value
4. Find max peak wind speed from a groupby

## Merlin-Cloud-Ground (Lightning)
1. Bin times to 15 minute windows
2. avg signal strength
3. sum of strikes

## Rain
1. take averages 

## Field mill
1. take averages
