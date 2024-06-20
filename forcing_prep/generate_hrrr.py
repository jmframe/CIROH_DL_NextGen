'''
Process HRRR data with CAMELS basins

'''


import argparse
import yaml
from multiprocessing.pool import ThreadPool
from pathlib import Path

import pandas as pd

import dask
import dask.delayed
import geopandas as gpd
import numpy as np
import s3fs
import xarray as xr
from dask.diagnostics import ProgressBar


# from aggregate import window_aggregate
# from weights import get_all_cov, get_weights_df
# The custom functions
from hrrr_proc import prep_date_time_range, _map_open_files_hrrrzarr, _gen_hrrr_zarr_urls
from geo_proc import process_geo_data

dask.config.set(pool=ThreadPool(12))
import dask.dataframe as ddf

import pandas as pd
from functools import partial
from cartopy import crs as ccrs
import exactextract
import zarr # An xarray.open_mfdataset dependency
import rioxarray # dependency in exactextract
import rasterio # dependency in exactextract

def _preprocess_sel_time(xda, apcp_fcst):
    # This helps select the forecast hour of interest, rather than grab all forecasted hours
    # reference: how to pass arguments in preprocess:  https://github.com/pydata/xarray/pull/6825
    xda = xda.isel(time=apcp_fcst)
    return xda

# def _gen_hrrr_zarr_urls(date, level_vars_anl = None, level_vars_fcst=None, fcst_hr=0, bucket_subf = 's3://hrrrzarr/sfc'):
#     # Zarr data file structure follows e.g. 'hrrrzarr/sfc/20240430/20240430_22z_anl.zarr/2m_above_ground/TMP/2m_above_ground'
#     fs = s3fs.S3FileSystem(anon=True)
#     bucket_subfolder_date = f'{bucket_subf}/{date}/'
#     try:
#         times_anl = [x for x in fs.ls(bucket_subfolder_date) if '_anl.zarr' in x]
#         if not times_anl:
#             raise(Warning(f'Could not list bucket for {date} inside {bucket_subfolder_date}. Skipping.'))
#         urls_anl = _build_zarr_urls(times_anl, level_vars_anl)
#         # Build forecast urls based on forecast hour
#         times_avail_fcst = [x for x in fs.ls(bucket_subfolder_date) if '_fcst.zarr' in x]
#         times_fcst = _fcst_url_find(times_avail_fcst, fcst_hr, bucket_subf, date)
#         urls_fcst = _build_zarr_urls(times_fcst, level_vars_fcst)
#     except:
#         urls_fcst = list()
#         urls_anl = list()
#     return urls_fcst, urls_anl

if __name__ == "__main__":

    # parser = argparse.ArgumentParser(description='Process the YAML config file.')
    # parser.add_argument('config_path', type=str, help='Path to the YAML configuration file')
    # args = parser.parse_args()
    
    # # Load the YAML configuration file
    # with open(args.config_path, 'r') as file:
    #     config = yaml.safe_load(file)

    # cvar = config['cvar']
    # ctime_max = config['ctime_max']
    # cid = config['cid']
    # redo = config['redo']
    # x_lon_dim = config['x_lon_dim']
    # y_lat_dim = config['y_lat_dim']
    # out_dir = Path(config['out_dir'].format(home_dir=str(Path.home())))

    time_bgn = '2018-08-13'# '2018-07-13'
    time_end = '2024-04-30'

    level_vars_anl = ['2m_above_ground/TMP',
                '2m_above_ground/SPFH','surface/DLWRF',
                'surface/DSWRF','surface/PRES',
                '10m_above_ground/UGRD','10m_above_ground/VGRD']
    level_vars_fcst = ['surface/APCP_1hr_acc_fcst']
    apcp_fcst_hr = 0 # TODO add
    _bucket_subf = 's3://hrrrzarr/sfc'
    drop_vars = ['forecast_period','forecast_reference_time','height', 'pressure'] # Ignore these variables when merging forecast and nowcast xarray.Dataset objects. Default should likely just be ['forecast_period','forecast_reference_time'] 
    _hydrofab_source = "s3://lynker-spatial/hydrofabric/v20.1/camels/"
    _basin_url = _hydrofab_source + "Gage_{}.gpkg"
    basins = [1022500]

    y_lat_dim = 'projection_y_coordinate'
    x_lon_dim = 'projection_x_coordinate'

    out_dir = f'{Path.home()}/noaa/data/hrrr/redo'

    Path.mkdir(Path(out_dir), exist_ok = True)

    # Define the partial function used for processing time in forecast data:
    partial_func = partial(_preprocess_sel_time, apcp_fcst = apcp_fcst_hr)

    all_dates, all_hours = prep_date_time_range(time_bgn, time_end)
    gpkgs = [_basin_url.format(id) for id in basins]


    # HRRR grid uses the Lambert Conformal projection:
    proj = ccrs.LambertConformal(central_longitude=262.5, 
                                    central_latitude=38.5, 
                                    standard_parallels=(38.5, 38.5),
                                        globe=ccrs.Globe(semimajor_axis=6371229,
                                                        semiminor_axis=6371229))


    fs = s3fs.S3FileSystem(anon=True)

    for b in basins:

        # read the geopackage from s3
        gdf = gpd.read_file(
            fs.open(_basin_url.format(b)), driver="gpkg", layer="divides").to_crs(proj)

        for date in all_dates:
            print(f'Processing basin {b} on {date}')
            try:
                urls_fcst, urls_anl =  _gen_hrrr_zarr_urls(date=date, level_vars_anl=level_vars_anl, level_vars_fcst=level_vars_fcst,fcst_hr=apcp_fcst_hr, bucket_subf = _bucket_subf)
                #urls_fcst, urls_anl = _gen_hrrr_zarr_urls(date, level_vars_anl, level_vars_fcst,apcp_fcst_hr, _bucket_subf)
            except:
                raise ValueError(f'Could not list bucket for {date} inside {_bucket_subf}.\nConsider sf.ls() in lieu of explicit build.')

            skip_fcst = skip_anl = False
            if len(urls_fcst) == 0 == len(urls_anl) == 0:
                print(f'No data exist for {date}') 
                continue
            elif len(urls_fcst) == 0:
                print(f'No forecasted precip data available on {date}')
                skip_fcst = True
            elif len(urls_anl) == 0:
                print(f'No analysis data available on {date}')
                skip_anl = True
            elif len(urls_fcst[0]) == 0:
                raise Warning(f'No forecast urls exist for {date}') # e.g. '20180711'

            # Now run a data pull
            try:
                if not skip_anl:
                    dat_anl = _map_open_files_hrrrzarr(urls_ls = urls_anl, concat_dim = ['time',None])
                else: 
                    dat_anl = xr.Dataset()
                if not skip_fcst:
                    dat_fcst = _map_open_files_hrrrzarr(urls_ls = urls_fcst, concat_dim = ['time',None], preprocess = partial_func)
                else:
                    dat_fcst = xr.Dataset()
            except: # Example: 20190506
                print(f'Initial hrrrzarr file opening unsuccessful on {date}. Waiting 30s and reattempting:') 
                import time
                time.sleep(30) # wait 30 seconds and try again
                try:
                    if not skip_anl:
                        dat_anl = _map_open_files_hrrrzarr(urls_ls = urls_anl, concat_dim = ['time',None])
                    else: 
                        dat_anl = xr.Dataset()
                    if not skip_fcst:
                        dat_fcst = _map_open_files_hrrrzarr(urls_ls = urls_fcst, concat_dim = ['time',None], preprocess = partial_func)
                    else:
                        dat_fcst = xr.Dataset()
                except:
                    raise ValueError(f'TODO figure out what to do for {date}') 

            dat_anl = dat_anl.drop_vars([x for x in dat_anl.data_vars.keys() if x in drop_vars])
            dat_fcst = dat_fcst.drop_vars([x for x in dat_fcst.data_vars.keys() if x in drop_vars])
            forcing = dat_anl.merge(dat_fcst)   

            df = process_geo_data(gdf, data=forcing, name = b, y_lat_dim = y_lat_dim, x_lon_dim = x_lon_dim, out_dir = out_dir, redo = True)
            df = df.to_dataframe()
                
            save_path_base = f'{out_dir}/camels_{b}_{date}'

            # print(df)
            cats = df.groupby("divide_id")
            path = Path(save_path_base)
            Path.mkdir(path, exist_ok=True)
            for name, data in cats:
                data.to_csv(path / f"{name}.csv")
            agg = df.groupby("time").mean()
            agg.to_csv(path / f"camels_{b}_agg.csv")