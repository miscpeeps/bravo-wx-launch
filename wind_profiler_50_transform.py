import pandas as pd
import numpy as np
from datetime import timedelta, datetime

def time_date(row):
    row['datetime'] = f"{row['Event Date']} {row['Event Time']}"
    row['datetime'] = pd.to_datetime(row['datetime'])
    return row

def direction_sep(row):
    row['Wind Direction']=np.radians(row['Wind Direction'])
    row['Direction_x']=np.sin(row['Wind Direction'])
    row['Direction_y']=np.cos(row['Wind Direction'])
    return row

def wind_profiler_50(path,launchtime):
    init_df=pd.read_csv(path)
    time_init=launchtime-timedelta(hours=4)
    time_list=[time_init]
    while time_list[-1] < launchtime:
        time_list.append(time_list[-1]+timedelta(minutes=5))
    #load in the relevant csv
    #separate out the wind directions based on the angle provided
    init_df=init_df.apply(direction_sep,axis=1)
    #copy out the event date, time, profiler, altitude, speed, shear, WW direction in x and y directions
    wp_50_df=init_df.loc[:,['Event Date','Event Time','Wind Shear','Altitude','Wind Speed','Direction_x','Direction_y','WW']]
    #adjust dates to create a datetime that matches the actual launch times
    wp_50_df = wp_50_df.apply(time_date, axis=1)
    offset=(time_init-wp_50_df.iloc[0,-1])
    wp_50_df['datetime']=wp_50_df['datetime']+offset
    #get rid of irrelevant info
    wp_50_df.drop(['Event Date','Event Time'], axis=1,inplace=True)
    #create the output dataframe from the time_list
    output_df=pd.DataFrame(index=time_list)
    dummy_data=np.ones(49)*np.nan
    #create binning by height
    height_max=[0,5000,8000,11000,14000,17000,170000]
    #iterate through each bin
    for ind in range(len(height_max)-1):
        bin_name=f'Altitude Height: {height_max[ind+1]} '
        #filter out the relevant bin from the profiler dataframe
        bin_df=wp_50_df[(wp_50_df['Altitude'] < height_max[ind+1]) & (wp_50_df['Altitude'] > height_max[ind])].copy()

        #find the max speed for each datetime in the bin
        speed_df=bin_df.groupby(by='datetime').max()[['Wind Speed','Wind Shear','WW']]
        #backfill data, then forward fill the rest
        speed_df.fillna(method='bfill',inplace=True)
        speed_df.fillna(method='ffill',inplace=True)

        if speed_df.count()['Wind Speed'] > 0:
            latest_speed=speed_df
        else:
            speed_df=latest_speed

        #find the variance for the x and y components of wind direction
        dir_df=bin_df.groupby(by='datetime').var()[['Direction_x','Direction_y']]
        #backfill data, then forward fill the rest
        dir_df.fillna(method='bfill',inplace=True)
        dir_df.fillna(method='ffill',inplace=True)
        #sum the variances
        dir_df['Direction Variance']=dir_df['Direction_x']+dir_df['Direction_y']
        dir_df.drop(['Direction_x','Direction_y'],axis=1,inplace=True)

        if dir_df['Direction Variance'].count() > 0:
            latest_dir=dir_df
        else:
            dir_df=latest_dir

        #combine final solutions into one df, rename to relevant column names
        df=pd.concat([speed_df,dir_df],axis=1)
        df.rename(columns={"Wind Speed": f'{bin_name} m Speed (m/s)', "Direction Variance": f'{bin_name} m Direction (var)',"Wind Shear":f'{bin_name} m Shear',"WW":f'{bin_name} m WW?'},inplace=True)
        output_df=output_df.merge(df,left_index=True,right_index=True)

    #interpolate between missing times
    missing_times=list(set(time_list)-set(output_df.index.to_list()))
    data_interp=pd.DataFrame(index=missing_times,columns=output_df.columns)
    interp_df=pd.concat([output_df,data_interp])
    interp_df.sort_index(inplace=True)
    interp_df.interpolate(inplace=True)

    return interp_df