#!/usr/bin/env python
"""generate.py
Entrypoint for resampling zarr based aorc to hy_features catchments

@author: Nels Frazier <nfrazier@lynker.com>
@author: Guy Litt <glitt@lynker.com>
@version: 0.2
@example: python /Users/guylitt/git/CIROH_DL_NextGen/forcing_prep/generate.py "/Users/guylitt/git/CIROH_DL_NextGen/forcing_prep/config_aorc.yaml" 
Changelog/Contributions
 - version 0.1, originally created, NF
 - version 0.2, added yaml config, configurable arguments, minor bugfixes, GL

"""
import argparse
import yaml
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


def process_geo_data(gdf, data, name, y_lat_dim, x_lon_dim,out_dir = '', redo = False, cvar = 8, ctime_max = 120, cid = -1):
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
    save = Path(f"{out_dir}/{name}_coverage.parquet")
    if save.exists() and redo == False:
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process the YAML config file.')
    parser.add_argument('config_path', type=str, help='Path to the YAML configuration file')
    args = parser.parse_args()
    
    # Load the YAML configuration file
    with open(args.config_path, 'r') as file:
        config = yaml.safe_load(file)
    

    # Assign variables from the YAML file
   
    _aorc_source = config['aorc_source']
    _aorc_year_url = config['aorc_year_url_template']
    _basin_url = config['basin_url_template']
    basins = config['basins']
    years = tuple(config['years'])  # Convert list to tuple for years
    cvar = config['cvar']
    ctime_max = config['ctime_max']
    cid = config['cid']
    redo = config['redo']
    x_lon_dim = config['x_lon_dim']
    y_lat_dim = config['y_lat_dim']
    out_dir = Path(config['out_dir'].format(home_dir=str(Path.home())))


    if 'all' in basins:
        # Expected format: 's3://lynker-spatial/hydrofabric/v20.1/camels/Gage_{basin_id}.gpkg'
        # base_path = 's3://lynker-spatial/hydrofabric/v20.1/camels/'
        # List all the basins inside the hydrofabric s3 bucket path
        base_path = str(Path(_basin_url).parent)
        if 's3://' not in base_path:
            base_path = str(base_path).replace('s3:/','s3://')
        fs = s3fs.S3FileSystem(anon=True)
        basins = np.unique([Path(x).stem.split('_')[1] for x in  fs.ls(base_path) if '/Gage_' in x])


    # Create output directory in case it does not exist
    if not Path.exists(out_dir):
        print("Creating the following path for writing output: " + out_dir)
        Path.mkdir(out_dir, exist_ok = True, parents = True)
        
    # _hydrofab_source = "s3://lynker-spatial/hydrofabric/v20.1/camels/"
    # _aorc_source = "s3://noaa-nws-aorc-v1-1-1km"
    # _aorc_year_url = "{source}/{year}.zarr"
    # _basin_url = _hydrofab_source + "Gage_{}.gpkg"
    # basins = [1022500]
    # # the zarr data is formatted as one year per bucket, and each var an object
    # years = (2018, 2019)  # end year +1, this is effetively a single year???

    # cvar = 8
    # ctime_max = 120
    # cid = -1
    # redo = False
    # x_lon_dim = 'longitude'
    # y_lat_dim = 'latitude'
    # out_dir = f'{Path.home()}/noaa/data/aorc'
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

    gpkgs = [_basin_url.format(basin_id=id) for id in basins]

    proj = forcing[next(iter(forcing.keys()))].crs
    print(proj)

    for b in basins:
        # read the geopackage from s3
        gdf = gpd.read_file(
            _s3.open(_basin_url.format(basin_id=b)), driver="gpkg", layer="divides"
        ).to_crs(proj)

        df = process_geo_data(gdf, forcing, b, y_lat_dim = y_lat_dim, x_lon_dim = x_lon_dim, out_dir = out_dir, redo = redo,cvar = cvar, ctime_max =ctime_max, cid = cid)
        df = df.to_dataframe()
        # print(df)
        cats = df.groupby("divide_id")
        path = Path(f"{out_dir}/camels_{b}")
        Path.mkdir(path, exist_ok=True)
        for name, data in cats:
            data.to_csv(path / f"{name}.csv")
        agg = df.groupby("time").mean()
        agg.to_csv(path / f"camels_{b}_agg.csv")
