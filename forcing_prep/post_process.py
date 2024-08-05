import os
import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count

def get_unique_basin_id_with_leading_zeros(forcing_dir, year_start, year_end):
    # Define the list to store unique basin ids
    basin_ids = set()
    
    # Define the pattern to match the filenames
    file_pattern = f"{year_start}_to_{year_end}"
    
    # Iterate over files in the specified directory
    for filename in os.listdir(forcing_dir):
        if file_pattern in filename:
            # Extract the basin id (8 digit numerical prefix)
            basin_id = filename.split('_')[0]
            basin_ids.add(basin_id)
    
    # Return the sorted list of unique basin ids as strings
    return sorted(basin_ids)

def round_significant_figures(series, sigfigs):
    def round_value(x):
        if pd.isna(x) or x == 0:
            return x
        return round(x, sigfigs - int(np.floor(np.log10(abs(x)))) - 1)
    return series.apply(round_value)

def round_decimal_places(series, decimal_places):
    return series.round(decimal_places)

def reduce_significant_figures(args):

    basin_id, forcing_dir, year_start, year_end, rounding_specs = args
    
    print(f"{basin_id} in process")

    year_string = f"{year_start}_to_{year_end}"
    input_file = f"{forcing_dir}/{basin_id}_{year_string}_agg.csv"
    post_processed_dir = f"{forcing_dir}/post_processed"
    
    # Create post_processed directory if it doesn't exist
    os.makedirs(post_processed_dir, exist_ok=True)
    
    output_file = f"{post_processed_dir}/{basin_id}_{year_string}_agg_rounded.csv"
    
    # Check if the input file exists before proceeding
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return
    
    df = pd.read_csv(input_file)
    
    for variable, decimal_places in rounding_specs.items():
        if variable in df.columns:
            df[variable] = round_decimal_places(df[variable], decimal_places)
    
    # Save the complete rounded data
    df.to_csv(output_file, index=False)
    
    # Save each water year separately
    df['time'] = pd.to_datetime(df['time'])
    df['water_year'] = df['time'].apply(lambda x: x.year if x.month < 10 else x.year + 1)
    
    water_years = df['water_year'].unique()
    
    for water_year in water_years:
        df_water_year = df[df['water_year'] == water_year].copy()
        df_water_year.drop(columns=['water_year'], inplace=True)
        output_file_water_year = f"{post_processed_dir}/{basin_id}_{year_string}_agg_rounded_WR{water_year}.csv"
        df_water_year.to_csv(output_file_water_year, index=False)

    print(f"{basin_id} NOW FINISHED")

######## PARALLELIZABLE FUNCTION
def process_basin_id(args):
    return reduce_significant_figures(*args)

######## MAIN for running from command line
if __name__ == "__main__":
    forcing_dir = '/home/jmframe/data/CAMELS_US/wood_july2024/1980_to_2024/'
    year_start = '1980'
    year_end = '2024'

    rounding_specs = {
        "APCP_surface": 5,
        "DLWRF_surface": 4,
        "DSWRF_surface": 4,
        "PRES_surface": 3,
        "SPFH_2maboveground": 7,
        "TMP_2maboveground": 4,
        "UGRD_10maboveground": 4,
        "VGRD_10maboveground": 4
    }

    unique_basin_ids = get_unique_basin_id_with_leading_zeros(forcing_dir, year_start, year_end)

    # Prepare the arguments for parallel processing
    args = [(basin_id, forcing_dir, year_start, year_end, rounding_specs) for basin_id in unique_basin_ids]

    # Use all available CPUs
    with Pool(cpu_count()) as pool:
        pool.map(reduce_significant_figures, args)
