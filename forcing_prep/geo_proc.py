'''
@title: Process weights of raster grid data spanning catchment boundaries
@author: Nels Frazier
@author: Guy Litt
@description: Given a geodataframe representing catchment(s) boundaries and a raster dataset,
compute the mean data values spanning the catchment(s) boundaries.
@param: gdf  geodataframe of catchments
@param: data xarray dataset of raster data
@param:

# Changelog / contributions
    2024-May Originally created, NF
    2024-06-18 minor adaptations to flipped dataset check, data selection NF, GL

'''


import zarr
from pathlib import Path
from multiprocessing.pool import ThreadPool

import dask
import dask.delayed
import geopandas as gpd
import numpy as np
import s3fs
import xarray as xr
from dask.diagnostics import ProgressBar
import dask.dataframe as ddf

from aggregate import window_aggregate
from weights import get_all_cov, get_weights_df

def process_geo_data(gdf, data, name, y_lat_dim, x_lon_dim,out_dir = '', redo = False, cvar = 8, ctime_max = 120, cid = -1):
    print("Slicing data to domain")
    # Only need to load the raster for the geo data extent
    extent = gdf.total_bounds
    lats = slice(extent[1], extent[3])
    lons = slice(extent[0], extent[2])
    # In  case the data is upside down, flip the y axis
    flipped = bool(len(data[y_lat_dim]) > 1 and data[y_lat_dim][1] > data[y_lat_dim][0])
    if flipped:
        data = data.sel({y_lat_dim : slice(None, None, -1)})
        # in order for xarray to use slice indexing, need to ensure
        # the lats slice is high to low when the latitude index is reversed
        lats = slice(extent[3], extent[1])
    data = data.sel(indexers = {x_lon_dim:lons, y_lat_dim:lats})
    # Load or compute coverage masks
    save = Path(f"{out_dir}/{name}_coverage.parquet")
    if save.exists() and redo != True:
        print(f"Reading {name} coverage from file")
        coverage = ddf.read_parquet(save).compute()
    else:
        # If we don't have weights cached, compute and save them
        weight_raster = (
            data[next(iter(data.keys()))]
            .isel(time=0)
            .sel(indexers = {x_lon_dim:lons, y_lat_dim:lats})
            .compute()
        )
        print("Computing Weights")
        weights_df = get_weights_df(gdf, weight_raster)
        print("Creating Coverage")
        coverage = get_all_cov(data, weights_df, y_lat_dim = y_lat_dim, x_lon_dim = x_lon_dim)
        coverage.to_parquet(save)
    print("Processing the following raster data set")
    print(data)
    # Stack all the raster variables into a single multi-dimension array
    # This makes the windowing algorithm much more efficient as it can broadcast
    # operations arcoss all the variable data at once
    data = data.to_dataarray()


    # Chunk params were chosen based on processing HUC 01 (19k geometries) within reasonable
    # time and memory pressure.  These can have serious performance implications on large
    # geo data sets!!!
    ctime = np.min([ctime_max, len(data['time'])])
    
    # On huc01 when this is not 1, you get
    # KeyError: ('<this-array>-agg_xr5-1d8d7d6b0dd083c3658d89ffacb65555', 0, 0, 1)
    # when the results try to join :confused:
    # but seemed to work on on smaller domains (e.g. a camels basin)

    # Rechunk data through time, but ensure the entire spatial extent is in mem
    data = data.chunk(
        {"variable": cvar, y_lat_dim: -1, x_lon_dim: -1, "time": ctime}
    )
    # Build the template data array for the outputs
    coords = {
        "time": data.time,
        "divide_id": gdf["divide_id"].sort_values(),
        "variable": data.coords["variable"].values,
    }
    dims = ["variable", "time", "divide_id"]
    shp = (
        len(data.coords["variable"]),
        data.time.size,
        len(gdf["divide_id"]),
    )
    var = xr.DataArray(np.zeros(shp), coords=coords, dims=dims)
    # It is important to make sure these chunks align with the data chunks!
    var = var.chunk({"variable": cvar, "time": ctime, "divide_id": cid})
    result = data.map_blocks(window_aggregate, args=(coverage,), template=var)
    # Perform the computations
    with ProgressBar():
        result = result.compute()
    # Unstack the variables back into a dataset
    result = result.to_dataset(dim="variable")
    return result