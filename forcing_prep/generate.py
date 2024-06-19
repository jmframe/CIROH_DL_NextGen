#!/usr/bin/env python
"""generate.py
Entrypoint for resampling zarr based aorc to hy_features catchments

@author Nels Frazier <nfrazier@lynker.com>
@version 0.1
"""
from multiprocessing.pool import ThreadPool
from pathlib import Path

import dask
import dask.delayed
import geopandas as gpd
import numpy as np
import s3fs
import xarray as xr
from dask.diagnostics import ProgressBar

from aggregate import window_aggregate
from weights import get_all_cov, get_weights_df

dask.config.set(pool=ThreadPool(12))
import dask.dataframe as ddf


def process_geo_data(gdf, data, name):
    print("Slicing data to domain")
    # Only need to load the raster for the geo data extent
    extent = gdf.total_bounds
    lats = slice(extent[1], extent[3])
    lons = slice(extent[0], extent[2])
    # In  case the data is upside down, flip the y axis
    flipped = bool(len(data.latitude) > 1 and data.latitude[1] > data.latitude[0])
    if flipped:
        data = data.isel(latitude=slice(None, None, -1))
        # in order for xarray to use slice indexing, need to ensure
        # the lats slice is high to low when the latitude index is reversed
        lats = slice(extent[3], extent[1])
    data = data.sel(longitude=lons, latitude=lats)
    # Load or compute coverage masks
    save = Path(f"{name}_coverage.parquet")
    if save.exists():
        print(f"Reading {name} coverage from file")
        coverage = ddf.read_parquet(save).compute()
    else:
        # If we don't have weights cached, compute and save them
        weight_raster = (
            data[next(iter(data.keys()))]
            .isel(time=0)
            .sel(longitude=lons, latitude=lats)
            .compute()
        )
        print("Computing Weights")
        weights_df = get_weights_df(gdf, weight_raster)
        print("Creating Coverage")
        coverage = get_all_cov(data, weights_df)
        coverage.to_parquet(save)
    print("Processing the following raster data set")
    print(data)
    # Stack all the raster variables into a single multi-dimension array
    # This makes the windowing algorithm much more efficient as it can broadcast
    # operations arcoss all the variable data at once
    data = data.to_dataarray()

    # TODO move these chunk params to arguements/options
    # These were chosen based on processing HUC 01 (19k geometries) within reasonable
    # time and memory pressure.  These can have serious performance implications on large
    # geo data sets!!!
    ctime = 120
    cvar = 8
    # On huc01 when this is not 1, you get
    # KeyError: ('<this-array>-agg_xr5-1d8d7d6b0dd083c3658d89ffacb65555', 0, 0, 1)
    # when the results try to join :confused:
    # but seemed to work on on smaller domains (e.g. a camels basin)
    cid = -1
    # Rechunk data through time, but ensure the entire spatial extent is in mem
    data = data.chunk(
        {"variable": cvar, "latitude": -1, "longitude": -1, "time": ctime}
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


if __name__ == "__main__":
    _hydrofab_source = "s3://lynker-spatial/hydrofabric/v20.1/camels/"
    _aorc_source = "s3://noaa-nws-aorc-v1-1-1km"
    _aorc_year_url = "{source}/{year}.zarr"
    _basin_url = _hydrofab_source + "Gage_{}.gpkg"
    basins = [1022500]
    # the zarr data is formatted as one year per bucket, and each var an object
    years = (2018, 2019)  # end year +1, this is effetively a single year???

    # Setup the s3fs filesystem that is going to be used by xarray to open the zarr files
    _s3 = s3fs.S3FileSystem(anon=True)
    files = [
        s3fs.S3Map(
            root=_aorc_year_url.format(source=_aorc_source, year=year),
            s3=_s3,
            check=False,
        )
        for year in range(*years)
    ]

    forcing = xr.open_mfdataset(files, engine="zarr", parallel=True, consolidated=True)

    gpkgs = [_basin_url.format(id) for id in basins]

    proj = forcing[next(iter(forcing.keys()))].crs
    print(proj)

    # forcing = forcing.isel(time=range(24*30*6))

    for b in basins:
        # read the geopackage from s3
        gdf = gpd.read_file(
            _s3.open(_basin_url.format(b)), driver="gpkg", layer="divides"
        ).to_crs(proj)

        df = process_geo_data(gdf, forcing, b)
        df = df.to_dataframe()
        # print(df)
        cats = df.groupby("divide_id")
        path = Path(f"camels_{b}")
        Path.mkdir(path, exist_ok=True)
        for name, data in cats:
            data.to_csv(path / f"{name}.csv")
        agg = df.groupby("time").mean()
        agg.to_csv(path / f"camels_{b}_agg.csv")
