import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import logging
logger = logging.getLogger(__name__)


def time_date(row):
    row['datetime'] = f"{row['Event Date']} {row['Event Time']}"
    row['datetime'] = pd.to_datetime(row['datetime'])
    return row


def direction_sep(row):
    row['Direction'] = np.radians(row['Direction'])
    row['Direction_x'] = np.sin(row['Direction'])
    row['Direction_y'] = np.cos(row['Direction'])
    return row


def wind_profiler_915(path, launchtime):
    final_labels=['RWP0004 Max Height: 0.8  km Speed (m/s)',
   'RWP0004 Max Height: 0.8  km Direction (var)',
   'RWP0004 Max Height: 1.5  km Speed (m/s)',
   'RWP0004 Max Height: 1.5  km Direction (var)',
   'RWP0004 Max Height: 10  km Speed (m/s)',
   'RWP0004 Max Height: 10  km Direction (var)',
   'RWP0005 Max Height: 0.8  km Speed (m/s)',
   'RWP0005 Max Height: 0.8  km Direction (var)',
   'RWP0005 Max Height: 1.5  km Speed (m/s)',
   'RWP0005 Max Height: 1.5  km Direction (var)',
   'RWP0005 Max Height: 10  km Speed (m/s)',
   'RWP0005 Max Height: 10  km Direction (var)',
   'RWP0001 Max Height: 0.8  km Speed (m/s)',
   'RWP0001 Max Height: 0.8  km Direction (var)',
   'RWP0001 Max Height: 1.5  km Speed (m/s)',
   'RWP0001 Max Height: 1.5  km Direction (var)',
   'RWP0001 Max Height: 10  km Speed (m/s)',
   'RWP0001 Max Height: 10  km Direction (var)',
   'RWP0002 Max Height: 0.8  km Speed (m/s)',
   'RWP0002 Max Height: 0.8  km Direction (var)',
   'RWP0002 Max Height: 1.5  km Speed (m/s)',
   'RWP0002 Max Height: 1.5  km Direction (var)',
   'RWP0002 Max Height: 10  km Speed (m/s)',
   'RWP0002 Max Height: 10  km Direction (var)',
   'RWP0003 Max Height: 0.8  km Speed (m/s)',
   'RWP0003 Max Height: 0.8  km Direction (var)',
   'RWP0003 Max Height: 1.5  km Speed (m/s)',
   'RWP0003 Max Height: 1.5  km Direction (var)',
   'RWP0003 Max Height: 10  km Speed (m/s)',
   'RWP0003 Max Height: 10  km Direction (var)']
        
    # create list of 5 minute increments starting at 4 hours before launch
    time_init = launchtime - timedelta(hours=4)
    time_list = [time_init]
    while time_list[-1] < launchtime:
        time_list.append(time_list[-1] + timedelta(minutes=5))
    # load in the relevant csv
    init_df = pd.read_csv(path)

    # separate out the wind directions based on the angle provided
    init_df = init_df.apply(direction_sep, axis=1)

    # copy out the event date, time, profiler, height, speed, direction in x direction
    try:
        wp_915_df = init_df.loc[:,
                ['Event Date', 'Event Time', 'Profiler', 'Height', 'Speed', 'Direction_x', 'Direction_y']]
        # adjust dates to create a datetime that matches the actual launch times
        wp_915_df = wp_915_df.apply(time_date, axis=1)
        offset = (time_init - wp_915_df.iloc[0, -1])
        wp_915_df['datetime'] = wp_915_df['datetime'] + offset
        # get rid of irrelevant info
        wp_915_df.drop(['Event Date', 'Event Time'], axis=1, inplace=True)

        # sample the unique profilers
        profilers = wp_915_df['Profiler'].unique()
        # create the output dataframe from the time_list
        output_df = pd.DataFrame(index=time_list)
        dummy_data = np.ones(49) * np.nan

        ##iterate through each profiler
        for profiler in profilers:
            sensor_df=pd.DataFrame(index=time_list)
            latest_speed=pd.DataFrame(index=time_list,data=dummy_data)
            latest_dir=pd.DataFrame(index=time_list,data=dummy_data)
            # create dataframe for each profiler, organized by datetime
            profiler_df = wp_915_df.loc[wp_915_df['Profiler'] == profiler].copy()
            profiler_df.sort_values(by=['Height', 'datetime'], inplace=True)
            profiler_df.reset_index(drop=True, inplace=True)

            # create binning by height
            height_max = [0, 0.8, 1.5, 10]

            # iterate through each bin
            for ind in range(len(height_max) - 1):
                bin_name = f'Max Height: {height_max[ind + 1]} '
                # filter out the relevant bin from the profiler dataframe
                bin_df = profiler_df[
                    (profiler_df['Height'] < height_max[ind + 1]) & (profiler_df['Height'] > height_max[ind])].copy()

                # find the max speed for each datetime in the bin
                speed_df = bin_df.groupby(by='datetime').max()['Speed']
                # backfill data, then forward fill the rest
                speed_df.fillna(method='bfill', inplace=True)
                speed_df.fillna(method='ffill', inplace=True)

                if speed_df.count() > 0:
                    latest_speed = speed_df
                else:
                    speed_df = latest_speed

                # find the variance for the x and y components of wind direction
                dir_df = bin_df.groupby(by='datetime').var()[['Direction_x', 'Direction_y']]
                # backfill data, then forward fill the rest
                dir_df.fillna(method='bfill', inplace=True)
                dir_df.fillna(method='ffill', inplace=True)
                # sum the variances
                dir_df['Direction Variance'] = dir_df['Direction_x'] + dir_df['Direction_y']
                dir_df.drop(['Direction_x', 'Direction_y'], axis=1, inplace=True)

                if dir_df['Direction Variance'].count() > 0:
                    latest_dir = dir_df
                else:
                    dir_df = latest_dir

                # combine final solutions into one df, rename to relevant column names
                df = pd.concat([speed_df, dir_df], axis=1)
                df.rename(columns={"Speed": f'{profiler} {bin_name} km Speed (m/s)',
                                   "Direction Variance": f'{profiler} {bin_name} km Direction (var)'}, inplace=True)
                sensor_df = sensor_df.merge(df, left_index=True, right_index=True)

            output_df=output_df.merge(sensor_df,left_index=True,right_index=True)

        # interpolate between missing times
        missing_times = list(set(time_list) - set(output_df.index.to_list()))
        data_interp = pd.DataFrame(index=missing_times, columns=output_df.columns)
        interp_df = pd.concat([output_df, data_interp])
        interp_df.sort_index(inplace=True)
        interp_df.interpolate(inplace=True)
        interp_df.fillna(method='bfill',inplace=True)

         #check to make sure we've got all of the right labels    
        missing_labels=list(set(final_labels)-set(interp_df.columns))
        if len(missing_labels)>0: 
            interp_df[missing_labels]=np.nan
    except:
        interp_df=pd.DataFrame(index=time_list)
        interp_df[final_labels]=np.nan

    if interp_df.isnull().sum().sum()) > 0:
        logging.warning("NaN values present in processed wind_50 data")
        
    return interp_df
