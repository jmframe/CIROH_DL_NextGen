"""weights.py
    Module for computing polygon based raster weights

    Author
    -------
    Nels Frazier <nfrazier@lynker.com>

    Version
    -------
    0.1
"""

import dask
import dask.array as da
import dask.dataframe as ddf
import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from exactextract import exact_extract

dask.config.set({"dataframe.convert-string": False})


def get_weights_df(gdf: gpd.GeoDataFrame, raster: xr.DataArray,id_col:str = 'divide_id') -> pd.DataFrame:
    """Get the coverage weights of the given raster for each feature"""
    stats = ["cell_id", "coverage"]
    # raster must be xarray.core.dataarray.DataArray or list thereof
    output = exact_extract(
        raster,
        gdf,
        stats,
        include_cols=[id_col],
        output="pandas",
    )
    output.set_index(id_col, inplace=True)
    # Some features may have no coverage, in that case warn the user
    # and for now just do a nearest neighbor assignment so they have
    # SOME data...
    missing = output[output["cell_id"].apply(lambda x: x.size == 0)]
    if not missing.empty:
        print("WARNING the following features couldn't be extracted by exact extract: ")
        print(missing.index)
        print("Assigning them to nearest neighbor values")
        nearest = gdf[gdf[id_col].isin(missing.index)]
        gdf = gdf.drop(nearest.index)
        test = gpd.sjoin_nearest(gdf, nearest, how="right", distance_col="dist")
        mapping = test[["divide_id_left", "divide_id_right"]].groupby("divide_id_right")

        for name, group in mapping:
            copy_from = output.loc[group.iloc[0]["divide_id_left"]]
            output.loc[name] = copy_from

    # turns out this wasn't problem, but could be at some point...
    # TODO test exact extract's behavoir on features on the edge of the
    # raster domain...
    # clipped = gdf[ gdf['divide_id'].isin(missing['divide_id']) ]
    # rb = raster.rio.bounds()
    # clipped.loc[:,'geometry'] = clipped['geometry'].clip_by_rect(*rb)
    # clipped = exact_extract(
    #     raster,
    #     clipped,
    #     stats,
    #     include_cols=["divide_id"],
    #     output="pandas",
    # )
    # print(clipped)

    return output


def _build_index(series, global_shape):
    """
    From a given exact exact feature coverage row, unstack the encapsulated
    arrays and add an unraveled index based on global_shape
    """
    all = []
    for _, series in series.iterrows():
        cells = series["cell_id"]
        ids = pd.Series(da.from_array(cells), name="ids")
        cov = pd.Series(da.from_array(series["coverage"]), name="coverage")
        global_idx = np.unravel_index(cells, global_shape)
        idy = pd.Series(da.from_array(global_idx[0]), name="global_idx_y")
        idx = pd.Series(da.from_array(global_idx[1]), name="global_idx_x")
        did = pd.Series([series.name] * len(cells), name="divide_id")
        df = pd.concat([ids, cov, idy, idx, did], axis=1).set_index("divide_id")
        all.append(df)
    return pd.concat(all)


def get_all_cov(dataset, weights_df,y_lat_dim = 'latitude', x_lon_dim = 'longitude'):
    """
    From an exact extract weights dataframe, reshape the weights/coverage
    into flat dataframe keyed on divide_id. This dataframe also has the x,y
    raveled indicies for cell_id based on the extent of the provided dataset
    """
    # TODO allow npartitions to be more configurable
    dask_weights = ddf.from_pandas(weights_df, npartitions=8)
    meta = {
        "ids": np.int64,
        "coverage": float,
        "global_idx_y": np.int64,
        "global_idx_x": np.int64,
    }
    all = dask_weights.map_partitions(
        _build_index, (dataset[y_lat_dim].size, dataset[x_lon_dim].size), meta=meta
    )
    all = all.compute()

    return all
