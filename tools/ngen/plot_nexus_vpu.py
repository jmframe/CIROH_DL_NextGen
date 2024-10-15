import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
from multiprocessing import Pool
import matplotlib.colors as mcolors

# Set paths for input data
gpkg_dir = "/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/"
output_base_dir = "/home/jmframe/ngen/extern/lstm/ngen_output/"
output_plot_dir = "/home/jmframe/CIROH_DL_NextGen/tools/ngen/vpu_plots/"

def process_vpu(vpu):
    try:
        # Define paths
        gpkg_path = os.path.join(gpkg_dir, f"nextgen_{vpu}.gpkg")
        output_dir = os.path.join(output_base_dir, f"vpu{vpu}")

        # Check if directory exists
        if not os.path.exists(output_dir):
            print(f"Skipping VPU {vpu}: Directory not found")
            return None

        # Load the GeoPackage
        gdf = gpd.read_file(gpkg_path)

        # Initialize a dictionary to store the last nexus output values
        nexus_values = {}

        # Loop through files matching 'nex-*_output.csv'
        for filename in os.listdir(output_dir):
            if filename.startswith("nex-") and filename.endswith("_output.csv"):
                nexus_id = filename.split('-')[1].split('_')[0]
                df = pd.read_csv(os.path.join(output_dir, filename), header=None)

                # Extract the last value from the 3rd column, handling potential NaNs
                last_value = df.iloc[-1, 2]
                if pd.isna(last_value):
                    last_value = 0  # Set NaN values to zero

                nexus_values[nexus_id] = last_value

        # Convert the dictionary to a DataFrame for merging
        df_nexus_values = pd.DataFrame(list(nexus_values.items()), columns=["id", "last_value"])
        df_nexus_values["id"] = "nex-" + df_nexus_values["id"]

        # Merge with GeoPackage data
        gdf_merged = gdf.merge(df_nexus_values, on="id", how="left")
        gdf_merged["last_value"].fillna(0, inplace=True)  # Handle NaN values

        return gdf_merged

    except Exception as e:
        print(f"Error processing VPU {vpu}: {e}")
        return None

def rescale_alpha(values):
    """Rescale values to fall between 0.4 and 1 for alpha."""
    min_val = values.min()
    max_val = values.max()
    if max_val == min_val:
        return 1  # If all values are the same, return alpha=1
    return 0.1 + 0.6 * ((values - min_val) / (max_val - min_val))

def generate_intermediate_plot(gdf_merged, vpu):
    """Generate intermediate plot for each VPU."""
    fig, ax = plt.subplots(figsize=(10, 8))
    alphas = rescale_alpha(gdf_merged["last_value"])

    gdf_merged.plot(
        column="last_value",
        cmap="GnBu",
        legend=True,
        markersize=20,
        alpha=alphas,
        ax=ax,
    )

    plt.title(f"Nexus Points for VPU {vpu}, Colored by Last Output Value")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")

    # Save the intermediate plot
    plot_path = os.path.join(output_plot_dir, f"nexus_plot_{vpu}.png")
    plt.savefig(plot_path)
    plt.close()
    print(f"Successfully plotted VPU {vpu}")

def main():
    # List of all VPUs
    vpus = [
        "01", "02", "03N", "03W", "04", "05", "06", "07", "08",
        "09", "10L", "10U", "11", "12", "13", "14", "15", "16", "18"
    ]

    # Use multiprocessing to process all VPUs
    with Pool(processes=40) as pool:
        results = pool.map(process_vpu, vpus)

    # Generate intermediate plots for each VPU
    for idx, gdf_merged in enumerate(results):
        if gdf_merged is not None:
            generate_intermediate_plot(gdf_merged, vpus[idx])

    # Combine all GeoDataFrames for the final CONUS plot
    combined_gdf = gpd.GeoDataFrame(pd.concat([gdf for gdf in results if gdf is not None], ignore_index=True))

    # Plot the combined CONUS map
    fig, ax = plt.subplots(figsize=(15, 10))
    alphas = rescale_alpha(combined_gdf["last_value"])

    combined_gdf.plot(
        column="last_value",
        cmap="GnBu",
        legend=True,
        markersize=20,
        alpha=alphas,
        ax=ax,
    )

    plt.title("CONUS Nexus Points, Colored by Last Output Value")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")

    # Save the final CONUS plot
    plt.savefig(os.path.join(output_plot_dir, "nexus_plot_CONUS.png"))
    plt.close()
    print("Successfully generated the CONUS plot")

if __name__ == "__main__":
    main()



















# import geopandas as gpd
# import pandas as pd
# import matplotlib.pyplot as plt
# import os

# # Set paths for input data
# gpkg_dir = "/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/"
# output_base_dir = "/home/jmframe/ngen/extern/lstm/ngen_output/"

# def normalize(series):
#     """Normalize a pandas Series to the range [0, 1]."""
#     return (series - series.min()) / (series.max() - series.min())

# def process_all_vpus(vpus):
#     # Initialize an empty GeoDataFrame for merging all VPUs
#     full_gdf = gpd.GeoDataFrame()

#     # Loop through each VPU
#     for vpu in vpus:
#         print(f"Processing VPU: {vpu}")

#         # Define paths
#         gpkg_path = os.path.join(gpkg_dir, f"nextgen_{vpu}.gpkg")
#         output_dir = os.path.join(output_base_dir, f"vpu{vpu}")

#         # Load the GeoPackage
#         gdf = gpd.read_file(gpkg_path)

#         # Initialize a dictionary to store the last nexus output values
#         nexus_values = {}

#         # Loop through each 'nex-*_output.csv' file in the VPU directory
#         for filename in os.listdir(output_dir):
#             if filename.startswith("nex-") and filename.endswith("_output.csv"):
#                 # Extract nexus ID from the filename
#                 nexus_id = filename.split('-')[1].split('_')[0]

#                 # Read the last line of the CSV file to get the final value
#                 file_path = os.path.join(output_dir, filename)
#                 df = pd.read_csv(file_path, header=None)

#                 # Extract the last value from the 3rd column
#                 last_value = df.iloc[-1, 2]

#                 # Store the value with the corresponding nexus ID
#                 nexus_values[nexus_id] = last_value

#         # Convert the dictionary to a DataFrame for merging with the GeoPackage
#         df_nexus_values = pd.DataFrame(list(nexus_values.items()), columns=["id", "last_value"])
#         df_nexus_values["id"] = "nex-" + df_nexus_values["id"]  # Add 'nex-' prefix to match GeoPackage IDs

#         # Merge GeoPackage data with the nexus output values
#         gdf_merged = gdf.merge(df_nexus_values, on="id", how="left")

#         # Append the merged GeoDataFrame to the full GeoDataFrame
#         full_gdf = pd.concat([full_gdf, gdf_merged], ignore_index=True)

#     # Normalize the last_value column to use for transparency (alpha)
#     full_gdf["alpha"] = normalize(full_gdf["last_value"])

#     # Plot the full CONUS map with value-based transparency
#     fig, ax = plt.subplots(figsize=(20, 15))
#     full_gdf.plot(
#         column="last_value",  # Use the last value for color mapping
#         cmap="coolwarm",  # Colormap for visualization
#         legend=True,
#         markersize=20,
#         alpha=full_gdf["alpha"],  # Apply transparency based on the normalized values
#         ax=ax
#     )

#     # Add labels and title
#     plt.title("CONUS Nexus Points, Colored by Last Output Value")
#     plt.xlabel("Longitude")
#     plt.ylabel("Latitude")

#     # Save the full CONUS plot
#     plt.savefig("conus_nexus_plot.png")
#     plt.close()

# if __name__ == "__main__":
#     # List of all VPUs to process
#     VPUs = ["01", "02", "03N", "03S", "04", "05", "06", "07", "08", 
#             "09", "10L", "10U", "11", "12", "13", "14", "15", "16", "17", "18"]

#     # Process all VPUs and generate the full CONUS plot
#     process_all_vpus(VPUs)





# import geopandas as gpd
# import pandas as pd
# import matplotlib.pyplot as plt
# import os
# import sys

# # Set paths for input data
# gpkg_dir = "/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/"
# output_base_dir = "/home/jmframe/ngen/extern/lstm/ngen_output/"

# def process_vpu(vpu):
#     # Define paths
#     gpkg_path = os.path.join(gpkg_dir, f"nextgen_{vpu}.gpkg")
#     output_dir = os.path.join(output_base_dir, f"vpu{vpu}")
#     print(output_dir)

#     # Load the GeoPackage
#     gdf = gpd.read_file(gpkg_path)

#     # Initialize a dictionary to store the last nexus output values
#     nexus_values = {}

#     # Loop through each file in the VPU directory that matches the pattern 'nex-*_output.csv'
#     for filename in os.listdir(output_dir):
#         if filename.startswith("nex-") and filename.endswith("_output.csv"):
#             # Extract nexus ID from the filename
#             nexus_id = filename.split('-')[1].split('_')[0]

#             # Read the last line of the CSV file to get the final value
#             file_path = os.path.join(output_dir, filename)
#             df = pd.read_csv(file_path, header=None)

#             # Extract the last value from the 3rd column
#             last_value = df.iloc[-1, 2]

#             # Store the value with the corresponding nexus ID
#             nexus_values[nexus_id] = last_value

#     # Convert the dictionary to a DataFrame for merging with the GeoPackage
#     df_nexus_values = pd.DataFrame(list(nexus_values.items()), columns=["id", "last_value"])
#     df_nexus_values["id"] = "nex-" + df_nexus_values["id"]  # Add 'nex-' prefix to match GeoPackage IDs

#     # Merge GeoPackage data with the nexus output values
#     gdf_merged = gdf.merge(df_nexus_values, on="id", how="left")

#     # Plot the GeoPackage points, colored by the last nexus output values
#     fig, ax = plt.subplots(figsize=(10, 8))
#     gdf_merged.plot(
#         column="last_value",  # Use the last value for color mapping
#         cmap="coolwarm",  # Colormap for visualization
#         legend=True,
#         markersize=20,
#         ax=ax
#     )

#     # Add labels and title
#     plt.title(f"Nexus Points for {vpu}, Colored by Last Output Value")
#     plt.xlabel("Longitude")
#     plt.ylabel("Latitude")

#     # Save the plot
#     plt.savefig(f"nexus_plot_{vpu}.png")
#     plt.close()

# if __name__ == "__main__":
#     # Get the VPU from the command-line argument
#     vpu = sys.argv[1]
#     print(f"Processing VPU: {vpu}")
#     process_vpu(vpu)