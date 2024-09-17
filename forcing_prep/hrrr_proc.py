'''
A collection processing zarr-formatted HRRR datasets 

'''
import pandas as pd
import numpy as np
import s3fs
from pathlib import Path
import datetime as dt
from cartopy import crs as ccrs
import dask
import dask.delayed
import xarray as xr
import warnings

def prep_date_time_range(time_bgn, time_end):
    '''
    Prepare range of dates as a list, formatted YMD based on HRRR urls
    '''
    time_range = pd.date_range(start =time_bgn, end = time_end, freq = 'D')
    dates = [str(dt.year)+ f'{dt.month:02d}' + f'{dt.day:02d}' for dt in time_range]
    all_hours = [f"{num:02d}" for num in range(24)]
    #hour = '00' # The forecast hour *Note that some hours are missing!!
    return dates, all_hours

def _build_zarr_urls(zarr_paths, level_vars):
    ls_urls = list()
    for levvara in level_vars:
        leva = levvara.split('/')[0]
        vara = levvara.split('/')[1]
        dat_urls1 = [f'{x}/{leva}/{vara}/{leva}' for x in zarr_paths] # data
        dat_urls2 = [f'{x}/{leva}/{vara}' for x in zarr_paths] # metadata
        zip_urls = [x for x in zip(dat_urls1,dat_urls2)]
        #zip_urls = list(map(list, zip(dat_urls1, dat_urls2)))
        ls_urls.append(zip_urls)
    return ls_urls

def _fcst_url_find(times_avail, fcst_hr, bucket_subf, date):
    '''
    The forecast data represent time into the future and this changes the date/hour to align with nowcast data.
    For example if forecast hour = 03, HRRR data extract the forecasted data from hours spanning 03 to 04,
        e.g. accumulated precipitation after one hour has passed by 04:00. 
    '''
    fs = s3fs.S3FileSystem(anon=True)
    # Find the url date - hour pairings corresponding to the desired forecasting hour
    if int(fcst_hr) > 23:
        raise ValueError('Forecast data subsetting has only been designed for forecasts up to 24 hours in advance. Please set fcst_hr to values from 0 to 23.')

    times_fcst_og = [x for x in times_avail if '_fcst.zarr' in x] # The standard URLS for a single day
    # Determine subset of day's timestamps desired
    str_max = f"_{24-(fcst_hr+1):02d}z_"
    idx_max = [x.item() for x in np.where(np.char.find(np.array(times_fcst_og), str_max) >-1) ]
    if len(idx_max) != 1:
        for x in np.arange(0,24-(fcst_hr+1))[::-1]:
            str_max = f"_{x:02d}z_"
            idx_max = [x.item() for x in np.where(np.char.find(np.array(times_fcst_og), str_max) >-1)]
            if len(idx_max) == 1:
                break
    sub_times_fcst_og = times_fcst_og[0:int(idx_max[0])]

    # Now find previous day's hours of interest:
    back_time = pd.to_datetime(date, format = '%Y%m%d') - pd.Timedelta(fcst_hr+1, unit='hour')
    back_date = str(back_time)[0:10].replace('-','')
    back_hour = f'_{24-(fcst_hr+1):02d}z_'
    bucket_subfolder_backdate = f'{bucket_subf}/{back_date}/'
    times_avail_back = [x for x in fs.ls(bucket_subfolder_backdate) if '_fcst.zarr' in x ]
    times_sel_back = list()
    
    for x in np.arange((24-(fcst_hr+1)),24):
        str_find_back = f'_{x:02d}z_'
        times_sel_back.append(times_avail_back[[x.item() for x in np.where(np.char.find(times_avail_back, str_find_back)>-1)][0]])
    urls_fcst = times_sel_back + sub_times_fcst_og
    return urls_fcst

def _gen_hrrr_zarr_urls(date, level_vars_anl = None, level_vars_fcst=None, fcst_hr=0, bucket_subf = 's3://hrrrzarr/sfc'):
    # Zarr data file structure follows e.g. 'hrrrzarr/sfc/20240430/20240430_22z_anl.zarr/2m_above_ground/TMP/2m_above_ground'
    fs = s3fs.S3FileSystem(anon=True)
    bucket_subfolder_date = f'{bucket_subf}/{date}/'
    try:
        times_anl = [x for x in fs.ls(bucket_subfolder_date) if '_anl.zarr' in x]
        if not times_anl:
            raise(Warning(f'Could not list bucket for {date} inside {bucket_subfolder_date}. Skipping.'))
        urls_anl = _build_zarr_urls(times_anl, level_vars_anl)
        # Build forecast urls based on forecast hour
        times_avail_fcst = [x for x in fs.ls(bucket_subfolder_date) if '_fcst.zarr' in x]
        times_fcst = _fcst_url_find(times_avail_fcst, fcst_hr, bucket_subf, date)
        urls_fcst = _build_zarr_urls(times_fcst, level_vars_fcst)
    except:
        urls_fcst = list()
        urls_anl = list()
    return urls_fcst, urls_anl

def _check_hrrrzarr_url_time_vs_data_time(urls_ls, ls_vars,drop_vars=None,fcst_hr = 0):
    '''
    Checks to see if any timestamps specified in a zarr url do not agree
    with timestamps retrieved in data, and provides new data if needed.

    Parameters
    ----------
    urls_ls : list
        A list of urls organized by var[timebythehour[zarr url, metadata url]].
    ls_vars : list
        A list of datasets, each list item unique to a HRRR variable.
    fcst_hr : int, optional
        Number of hours into the future of a forecast, used in generating urls_ls and processing ls_vars. Default is 0.

    Returns
    -------
    tuple
        subdat_ls, ls_wrong_ts where subdat_ls is a new version of ls_vars, and ls_wrong_ts is a list of boolean if any variable changed.

    See Also
    --------
    _map_open_files_hrrrzarr

    Changelog
    ---------
    2024-06-25: Adapt time concurrence check to account for forecast hour, GL

    '''
    ctr_ls = -1
    subdat_ls = list()
    ls_wrong_ts = list()
    for subdat in ls_vars:
        # First check to see that time index is present. If not, skip the variable (e.g. 20201124 PRES):
        if 'time' not in subdat.dims:
            continue      

        if drop_vars != None:
            subdat = subdat.drop_vars([x for x in subdat.data_vars.keys() if x in drop_vars])
        ctr_ls+=1
        # Extract timestamps in url:
        idx_chck = list()

        for day_url in  urls_ls[ctr_ls]:
            # Convert a url into a timestamp, acknowledging that for forecast hours, the forecast needs to be added back in to jive with the retrieved data's timestamp set using the 'preprocess' function arg passed inside _map_open_files_hrrrzarr()
            dt_url = pd.to_datetime(day_url[0].split('/')[3][0:11],format='%Y%m%d_%H') + pd.Timedelta(hours = fcst_hr)
            idx_chck.append(np.where(subdat.time.data == dt_url))
            # Simplify the time indexes for keeps into a list of ints
            idx_keep = [y[0] for y in [x[0] for x in idx_chck if x[0].size > 0]]

        if len(idx_keep) < len(urls_ls[ctr_ls]):
            ls_wrong_ts.append(True)
        sub_ls_var = list()
        for var_name, sub_da in subdat.items(): # loop across e/ dataarray to subset by time
            sub_ls_var.append(sub_da.isel(time=idx_keep))
        subdat_ls.append(xr.merge(sub_ls_var))
    return subdat_ls, ls_wrong_ts


def fxn():
    # Goal: Suppress the following warning that is generated when running open_mfdataset
    # "No index created for dimension time because variable time is not a coordinate. To create an index for time, please first call `.set_coords('time')` on this object."
    warnings.warn("UserWarning", UserWarning)

def _map_open_files_hrrrzarr(urls_ls, concat_dim = ['time',None], preprocess = None,drop_vars = None,fcst_hr = 0):
    # Expect urls_ls to be a nested list as follows: [var[date-hour[paired urls]]]
    fs = s3fs.S3FileSystem(anon=True)
    # Map the urls
    files_map = [[[s3fs.S3Map(z , s3=fs) for z in y] for y in x] for x in urls_ls]
    # Problem: some variables needs to be read in using consolidated = False (e.g. DSWRF 20240430), which is much slower
    ls_vars = list()
    var_ctr = -1
    for fvar in files_map:
        var_ctr +=1
        with warnings.catch_warnings():
            # Suppress the UserWarning on 'time variable not a coordinate' that is generated when running open_mfdataset
            warnings.simplefilter("ignore")
            fxn()
            # DO NOT set consolidated = True as some variables need consolidated = False
            try:
                ls_vars.append(xr.open_mfdataset(fvar, engine = 'zarr', parallel = True,
                                            combine = 'nested',
                                            preprocess = preprocess,
                                            concat_dim = concat_dim)) 
            except:
                print('Could not successfully process urls. Attempting to process by the hour instead of day (this takes longer)')
                var = urls_ls[var_ctr]
                var_name = var[0][1].split('/')[-1]
                print(f'Problematic variable includes: {var_name}')
                
                ls_hrs = list()
                for hr in var: 
                    try:
                        hr_map = [s3fs.S3Map(h, s3=fs) for h in hr]
                        check = (xr.open_mfdataset(hr_map, engine = 'zarr', parallel = True,
                                        combine = 'nested',
                                        preprocess = preprocess # TODO may need to handle a different pre-processing function in case failure occurs
                                        ))  # Note no concat_dim when working with lowest 'resolution'
                        # Add in expected data variable check:
                        if not 'fcst' in var_name: # A forecast variable name likely does not have a 'time' variable, but rather a 'forecast_reference_time'
                            must_have_vars = [x for x in concat_dim if x is not None] + [var_name]
                        else: # Do not want 'time' to be expected in variables for the case of APCP_1hr_acc_fcst
                            must_have_vars = [var_name]

                        if not all([x in check.data_vars for x in must_have_vars]): # Example 20200318_22z DSWRF
                            # Expected variables include 'time' and the var_name
                            print(f'Skipping {var_name} because {' & '.join(must_have_vars)} are not present as variables in the dataset from {hr}. ')
                            continue
                    except: # Skip this one and head to the next
                        print(f'Could not process {hr}. Skipping. Consider the preprocess function or faulty zarr data.')
                        continue
                    else:
                        ls_hrs.append(check)
                # Concatenate each hour's dataset into a full days' dataset:
                if len(ls_hrs) > 0:
                    sub_concat = xr.concat(ls_hrs,dim='time')
                else:
                    sub_concat = xr.Dataset()
                ls_vars.append(sub_concat) # Add the variable's full day to list of variables
    try: # Upon building list of variable-days, combine into a single dataset
        # TODO On 20200727 why does everything go to 20200728T01 except for the variable PRES?
        # TODO consider fixing how the urls were generated (e.g. [(x.time.data[1]) for x in ls_vars])
        dat = xr.merge(ls_vars)
    except: 
        dat1 = xr.Dataset()
        dat2 = xr.Dataset()
        ctr = -1
        for ls in ls_vars: # Perform over each variable 
            if any(ls.get_index('time').duplicated()):
                 # Eg. 20200914 'cannot reindex or align along dimension 'time' because the pandas index has duplicate values
                 ls = ls.drop_duplicates(dim = ['time'], keep = 'last')
            ctr += 1
            print(ctr)
            try:
                dat1 = xr.merge([dat1,ls])
            except:
                try:
                    dat2 = xr.merge([dat2, ls])
                except: # If none of this data salvaging attempt works, just skip it
                    print("PROBLEM HERE")
           
        if len(dat2.keys()) >0:
            # Try to extract the data from dat2
            try:
                print("DO SOMETHING")
            except:
                print("DO SOMETHING 2")
        dat = dat1
    if any(dat.get_index('time').duplicated()): # Double check no timestamps were duplicated
        print(f'Duplicated timestamps generated - removed.')
        dat = dat.drop_duplicates(dim = ['time'], keep = 'last')

    # Run timestamp checker:
    subdat_ls, ls_wrong_ts = _check_hrrrzarr_url_time_vs_data_time(urls_ls, ls_vars,drop_vars,fcst_hr=fcst_hr)
    # When unexpected timestamps exist, _check_hrrrzarr_url_time_vs_data_time removes them (e.g. 20200727T01 should return 20200728T01 for TMP
    if any(ls_wrong_ts):
        # There are more timestamps than expected.
        dat = xr.merge(subdat_ls)
    return dat


