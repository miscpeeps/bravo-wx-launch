import datetime
from datetime import datetime as dt
import numpy as np
import pandas as pd

def time_date(row):
    row['datetime'] = f"{row['Event Date']} {row['Event Time']}"
    row['datetime'] = pd.to_datetime(row['datetime'])
    return row

def lowamps(path, launchtime):
    # load
    df = pd.read_csv(path)
    
    # merge event date and time
    df = df.apply(time_date, axis=1)
    
    # remove excess columns
    df = df.drop(columns=['Event Date', 'Event Time','Julian Date','Wind Direction', 'Wind Shear', 'Temperature', 'Dew Point', 'Pressure',
       'Relative Humidity', 'AbsoluteHumidity', 'Density', 'IndexOfRefraction',
       'VelocityOfSound', 'SaturationVaporPressure'])
    
    # ugly way to lump things into 5 minute groups
    inc = []
    index = -1
    for i in df.index:
        if i%5 == 0:
            index +=1
            inc.append(index)
        else:
            inc.append(index)
    
    # apply five minute lumps
    df['datetime'] = df['datetime'] - (np.multiply(inc, datetime.timedelta(minutes=-5)))
    
    # for fillna purposes
    first_value = df.iloc[0,:]
    
    # taking max
    groupby = df.groupby(by='datetime').max()
    
    # create empty dataframe in 5 minute time increments in the time zero
    l = (pd.DataFrame(columns=['NULL'],index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T')))
    
    # merge_asof groups nearby indices with a tolerance of 5 minutes
    groupby = pd.merge_asof(l, groupby, left_index=True, right_index=True, tolerance=pd.Timedelta("5m"))
    
    # created NULL column
    groupby.drop(columns='NULL', inplace=True)
    
    # fill NAs with ground value
    groupby = groupby.fillna(first_value)
    
    
    # CONTROVERSIAL - get rid of altitude
    groupby.drop(columns='Altitude', inplace=True)
    
    return groupby
