"""
Post-process HRRR data into individual dataframes of HRRR timeseries for each basin
Also mimic the netcdf structure used in the standard generate.py processing
 - In cases where divide_id does not exist as a column, the basin id is used as a placeholder

Perform this post-processing after running generate_hrrr.py

Usage:
python /path/to/post_process_hrrr.py "/path/to/config_hrrr_localgpkg.yaml" "/path/to/output/directory"
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
import xarray as xr
from generate import to_ngen_netcdf
import warnings

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

        # Rename to actual gage_id since the hydrofabric omits leading zeros
        if len(b) == 7: # Should be a total of 8 characters
            b = '0' + b
        name_compiled_df = f'HRRR_ts_gage_{b}.csv'
        
        path_compiled = Path(dir_write/Path(name_compiled_df))
        compiled_df.to_csv(path_compiled,  index=False)

        # ---------- Now process into the expected netcdf format ----------
        if 'divide_id' not in compiled_df.columns:
            warnings.warn(f'divide_id not inside dataset column for {b}. Assigning the basin id as a placeholder')
            div_ids = [b]*compiled_df.shape[0]
        else:
            div_ids = compiled_df['divide_id']
            compiled_df.drop('divide_id',axis=1, inplace=True)

        compiled_df.set_index('time', inplace=True)
        compiled_df.drop('index',axis=1,inplace=True)
        # compiled_df.set_index('divide_id', inplace=True)
        # TODO need to add divide_id as used in hydrofabric if it doesn't exist
        ds = xr.Dataset.from_dataframe(compiled_df)
        ds_exp = ds.expand_dims(divide_id=div_ids)
        # df_pivot = compiled_df.pivot(index='time', columns='divide_id', values=['TMP','SPFH','DLWRF','DSWRF','PRES','UGRD','VGRD','APCP_1hr_acc_fcst'])
        # ds = xr.Dataset.from_dataframe(df_pivot)

        uniq_name = 'HRRR_ts_gage_{b}'
        out_dir_ncdf = Path(out_dir/Path('netcdf'))
        out_dir_ncdf.mkdir(exist_ok=True)
        to_ngen_netcdf(ds = ds_exp, out_dir = out_dir_ncdf, uniq_name = uniq_name )
