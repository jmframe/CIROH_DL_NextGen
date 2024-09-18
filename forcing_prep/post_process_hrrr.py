"""
Post-process HRRR data into individual dataframes of HRRR timeseries for each basin

Perform after running generate_hrrr.py

Usage:
python /path/to/post_process_hrrr.py "/path/to/config_hrrr_localgpkg.yaml" "/path/to/output/directory"
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process the YAML config file.')
    parser.add_argument('config_path', type=str, help='Path to the YAML configuration file')
    parser.add_argument('dir_write',type= str, help="Directory location to write output")
    args = parser.parse_args()

    
    # Load the YAML configuration file
    with open(args.config_path, 'r') as file:
        config = yaml.safe_load(file)

    
    home_dir = Path.home()
    dir_write = Path(args.dir_write.format(home_dir=home_dir))
    out_dir = Path(config['out_dir'].format(home_dir=home_dir)) # out_dir = f'{Path.home()}/noaa/data/hrrr/redo'
    
    subdirs = [p for p in out_dir.rglob('*') if p.is_dir()]

    subfiles = [f for f in out_dir.rglob('*.parquet')] # Should contain all basin identifiers

    basin_ids = [f.name.replace('_coverage.parquet','') for f in subfiles]

    for b in basin_ids:
        paths_agg = [f for sd1 in subdirs for f in sd1.iterdir()  if f.is_file and b in f.name and 'agg.csv' in f.name]
        paths_non_agg = [f for sd1 in subdirs for f in sd1.iterdir()  if f.is_file and b in f.name and 'agg.csv' not in f.name]

        compiled_df = pd.concat([pd.read_csv(x) for x in paths_agg]).sort_values(by = 'time', ascending=True).reset_index()

        name_compiled_df = f'HRRR_ts_gage_{b}.csv'
        
        path_compiled = Path(dir_write/Path(name_compiled_df))
        compiled_df.to_csv(path_compiled,  index=False)
        

    