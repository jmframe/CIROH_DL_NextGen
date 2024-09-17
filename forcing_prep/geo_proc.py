'''Process weights of raster grid data spanning catchment boundaries.

    Authors
    -------
    Nels Frazier <nfrazier@lynker.com>
    Guy Litt <glitt@lynker.com>

    Changelog / Contributions
    -------------------------
    2024-May: Originally created, NF
    2024-06-18: Minor adaptations to flipped dataset check, data selection, NF, GL
    2024-06-27: Expand slicing dimension coverage if first attempt at computing weights fails, GL
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
    '''
   Given a geodataframe representing catchment(s) boundaries and a raster dataset,
    compute the mean data values spanning the catchment(s) boundaries.

    Parameters
    ----------
    gdf : GeoDataFrame
        Geodataframe of catchments.
    data : xarray.Dataset
        Xarray dataset of raster data.
    name : str
        A unique file name used for saving catchment-specific grid weights. The basin id is ideal.
    y_lat_dim : str
        The latitude identifier in the xarray dataset `data`.
    x_lon_dim : str
        The longitude identifier in the xarray dataset `data`.
    out_dir : str
        The desired save directory for catchment-specific grid weights.
    redo : bool, optional
        Should previously saved catchment grid weights be read in if they have already been created? If False, weights are regenerated. Default is False.
    cvar : int, optional
        Chunk size for variables. Default is 8.
    ctime_max : int, optional
        The max chunk time frame. Units of hours. Default is 120.
    cid : int, optional
        The divide_id chunk size. Default is -1, which means all divide_ids in a basin. A small value may be needed for very large basins with many catchments.

    Returns
    -------
    xr.dataset of retrieved variables
    '''
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
    data_sub = data.sel(indexers = {x_lon_dim:lons, y_lat_dim:lats})
    # Load or compute coverage masks
    save = Path(f"{out_dir}/{name}_coverage.parquet")
    if save.exists() and redo != True:
        print(f"Reading {name} coverage from file")
        coverage = ddf.read_parquet(save).compute()
        data = data_sub
        #NJF FIXME this isn't quite right if coverage is created based on biggerdata below?????
    else:
        # If we don't have weights cached, compute and save them
        weight_raster = (
            data_sub[next(iter(data_sub.keys()))]
            .isel(time=0)
            .sel(indexers = {x_lon_dim:lons, y_lat_dim:lats})
            .compute()
        )
        print("Computing Weights")
        try:
            weights_df = get_weights_df(gdf, weight_raster)
            data = data_sub
        except:
            print('weight_raster may not have enough coverage. Try expanding size of sliced raster')
            x_lon_diff = data[x_lon_dim][1].values - data[x_lon_dim][0].values
            y_lat_diff = data[y_lat_dim][1].values - data[y_lat_dim][0].values
            if flipped:
                lats_big = slice(extent[3]-y_lat_diff, extent[1]+y_lat_diff)
            else:
                lats_big = slice(extent[1]-y_lat_diff, extent[3]+y_lat_diff)
            lons_big = slice(extent[0]-x_lon_diff, extent[2]+x_lon_diff)
            biggerdata = data.sel(indexers = {x_lon_dim:lons_big, y_lat_dim:lats_big})
            weight_raster = (
                biggerdata[next(iter(biggerdata.keys()))]
                .isel(time=0)
                .sel(indexers = {x_lon_dim:lons_big, y_lat_dim:lats_big})
                .compute()
            )
            weights_df = get_weights_df(gdf, weight_raster)
            data = biggerdata
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
