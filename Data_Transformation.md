# Data Transforms

## Overall 

1. 5 Minute timesteps for the whole of the dataset
2. Generally groupby/bin time for sensors with less resolution and then consider how to effectively transform data
3. Rows will be columns in final data sets (in general) so data transformations to condense information are important and must be done surgically
4. Normalization is currently assumed to be done with fit() functions in sklearn, if not further considerations are needed for how to normalize

## Amps-low
1. For all row/columns, bin values into altitude groups [16, 1000-10000,10000-25000,25000+]
2. For wind direction/speed, vector add all values in one bin to capture average direction/speed
3. Average shear
4. Average temperature 
5. Discard dew point
6. Discard pressure
7. Discard both humidities
8. Discard density
9. Discard IndexOfRefraction
10. Discard Velocity of Sound 
11. Average PrecipitableWater

## Wind-Profiler 915 mhz

## Wind-Profiler 50 Mhz

## Weather-Tower

## Merlin-Cloud-Ground (Lightning)
