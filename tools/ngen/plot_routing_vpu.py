import geopandas as gpd
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import os
import matplotlib.colors as mcolors

# Paths for input and output data
gpkg_dir = "/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/"
output_base_dir = "/home/jmframe/ngen/extern/lstm/ngen_output/"
output_plot_dir = "/home/jmframe/CIROH_DL_NextGen/tools/ngen/vpu_plots/"

# List of VPUs
vpus = [
    "01", "02", "03N", "03S", "04", "05", "06", "07", "08", "09",
    "10L", "10U", "11", "12", "13", "14", "15", "16", "18"
]

def load_and_merge_vpu_data(timestep):
    """Load and merge all VPUs for the given timestep."""
    merged_gdf = gpd.GeoDataFrame()

    for vpu in vpus:
        try:
            # Load GeoPackage and NetCDF data
            gpkg_path = os.path.join(gpkg_dir, f"nextgen_{vpu}.gpkg")
            nc_file = os.path.join(output_base_dir, f"vpu{vpu}/troute_output_201601010000.nc")

            gdf = gpd.read_file(gpkg_path)
            ds = xr.open_dataset(nc_file)

            # Extract flow values for the specified timestep
            flow_values = ds["flow"].isel(time=timestep).values

            # Create a DataFrame to associate IDs with flow values
            df_flow = pd.DataFrame({
                "id": [f"nex-{id}" for id in ds["feature_id"].values],
                "flow": flow_values
            })

            # Merge flow values with GeoPackage geometries
            gdf_merged = gdf.merge(df_flow, on="id", how="left")
            gdf_merged["flow"].fillna(0, inplace=True)  # Handle NaNs

            # Append to the merged GeoDataFrame
            merged_gdf = pd.concat([merged_gdf, gdf_merged])

        except Exception as e:
            print(f"Error processing VPU {vpu}: {e}")

    return merged_gdf

def plot_conus_timestep(timestep):
    """Plot all VPUs together for a single timestep."""
    merged_gdf = load_and_merge_vpu_data(timestep)

    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 10))
    merged_gdf.plot(
        column="flow",
        cmap="GnBu",
        legend=True,
        markersize=10,
        ax=ax,
        alpha=0.7,
        norm=mcolors.LogNorm(vmin=merged_gdf["flow"].min() + 1e-6, vmax=merged_gdf["flow"].max())
    )

    # Add title and labels
    plt.title(f"CONUS Routing Flow at Timestep {timestep}")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")

    # Save the plot
    plt.savefig(os.path.join(output_plot_dir, f"conus_routing_timestep_{timestep}.png"))
    plt.close()
    print(f"Saved plot for timestep {timestep}")

if __name__ == "__main__":
    # Loop over all timesteps in the dataset (adjust range as needed)
    for timestep in range(0, 336):  # Assuming 336 timesteps (14 days, hourly)
        plot_conus_timestep(timestep)




# import geopandas as gpd
# import pandas as pd
# import xarray as xr
# import matplotlib.pyplot as plt
# import os
# import sys

# # Set paths for input data
# gpkg_dir = "/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/"
# output_base_dir = "/home/jmframe/ngen/extern/lstm/ngen_output/"
# output_plot_dir = "/home/jmframe/CIROH_DL_NextGen/tools/ngen/vpu_plots/"

# def plot_single_timestep(vpu, timestep=0):
#     try:
#         # Define paths
#         gpkg_path = os.path.join(gpkg_dir, f"nextgen_{vpu}.gpkg")
#         nc_file = os.path.join(output_base_dir, f"vpu{vpu}/troute_output_201601010000.nc")

#         # Load GeoPackage
#         gdf = gpd.read_file(gpkg_path)

#         # Load NetCDF data
#         ds = xr.open_dataset(nc_file)

#         # Extract flow values for the specified timestep
#         flow_values = ds["flow"].isel(time=timestep).values

#         # Create a DataFrame to associate IDs with flow values
#         df_flow = pd.DataFrame({
#             "id": [f"nex-{id}" for id in ds["feature_id"].values],
#             "flow": flow_values
#         })

#         # Merge flow values with GeoPackage geometries
#         gdf_merged = gdf.merge(df_flow, on="id", how="left")
#         gdf_merged["flow"].fillna(0, inplace=True)  # Handle NaNs

#         # Plot the merged data
#         fig, ax = plt.subplots(figsize=(10, 8))
#         gdf_merged.plot(
#             column="flow",
#             cmap="GnBu",
#             legend=True,
#             markersize=20,
#             ax=ax,
#             alpha=0.7
#         )

#         # Add title and labels
#         plt.title(f"Routing Flow for {vpu} at Timestep {timestep}")
#         plt.xlabel("Longitude")
#         plt.ylabel("Latitude")

#         # Save the plot
#         plt.savefig(os.path.join(output_plot_dir, f"routing_flow_{vpu}_timestep_{timestep}.png"))
#         plt.close()
#         print(f"Successfully plotted VPU {vpu} at timestep {timestep}")

#     except Exception as e:
#         print(f"Error processing VPU {vpu}: {e}")

# if __name__ == "__main__":
#     # Get VPU and timestep from command-line arguments
#     vpu = sys.argv[1]
#     timestep = int(sys.argv[2])  # Convert to integer
#     plot_single_timestep(vpu, timestep)