"""aggregate.py
Module for jagged window wieghted mean aggregration

@author Nels Frazier <nfrazier@lynker.com>
@version 0.1
"""

import numpy as np
import xarray as xr


def window_aggregate(dataset, coverage):
    """
    For each coverage window defined in coverage, grab the
    data from dataset for the coverage cells and do a weighted
    average for all times in the dataset.
    """
    print(f"Dataset shape: {dataset.shape}")
    all = []
    windows = coverage.groupby("divide_id")
    ids = coverage.index.unique().sort_values()

    for id in ids:
        window = windows.get_group(id)
        cov = window["coverage"]
        index = (window["global_idx_y"], window["global_idx_x"])
        # get the raw numpy data for window cells
        sub = dataset.values[:, :, index[0], index[1]]
        # perform the aggregation, axis 2 is time
        # axis 1 is variable
        data = np.sum(sub * cov.values, axis=2) / cov.sum()
        # create the dataarray to hold this aggregated data in
        # with the window id as a dim/coord
        da = xr.DataArray(
            data[:, :, np.newaxis],  # add a new axis for the divide_id
            dims=[
                "variable",
                "time",
                "divide_id",
            ],
            coords={
                "time": dataset.coords["time"],
                "variable": dataset["variable"].values,
                "divide_id": [id],
            },
        )
        # TODO don't append, lists get really large and may be leaking mem??
        # can allocate and define the final datarray upfront and just dump into it...
        all.append(da)

    ret = xr.concat(all, dim="divide_id")
    # Try to get rid of the data we don't need anymore, not sure this is
    # actually helping the memory pressure through
    del dataset
    return ret
