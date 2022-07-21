import datetime
from datetime import datetime as dt
import numpy as np
import pandas as pd

def time_date(row):
    row['datetime'] = f"{row['Event Date']} {row['Event Time']}"
    row['datetime'] = pd.to_datetime(row['datetime'])
    return row

def rainfall(path, launchtime):
    try:
        # load
        df = pd.read_csv(path)

        # merge date and time
        df = df.apply(time_date, axis=1)  
        
        # remove excess columns
        df.drop(columns=['Event Date', 'Event Time','Julian Day', 'Rain Gauge', 'IsActive'], inplace=True)
        
        groupby = df.groupby(by='datetime').max()

        groupby.rename(columns={'Inches':'Rain Gauge Inches'}, inplace=True)
        print(groupby)

          # create empty dataframe in 5 minute time increments in the time zero
        l = (pd.DataFrame(columns=['NULL'],index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T')))

          # merge_asof groups nearby indices with a tolerance of 5 minutes
        groupby = pd.merge_asof(l, groupby, left_index=True, right_index=True, tolerance=pd.Timedelta("5m"))
        

#          # created NULL column
        groupby.drop(columns='NULL', inplace=True)
        
    except:
        print(f"error populating rain gauge data for {launchtime}")
        groupby=pd.DataFrame(columns=['Rain Gauge Inches'], index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T'))
    groupby = groupby.fillna(0)
    
    
    return groupby
    
 