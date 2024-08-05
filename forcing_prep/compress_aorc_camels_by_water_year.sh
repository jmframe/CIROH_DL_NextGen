#!/bin/bash

# Directory containing the processed files
input_dir="/home/jmframe/data/CAMELS_US/wood_july2024/1980_to_2024/post_processed"

# Directory to store the tar files
output_dir="${input_dir}/compressed_warer_years"
mkdir -p "${output_dir}"

# Change to the directory with the files
cd "${input_dir}/water_years"

# Get a list of unique water years from the filenames
years=$(ls *_agg_rounded_WR*.csv | sed -n 's/.*_WR\([0-9]\+\).csv/\1/p' | sort -u)

# Loop through each year and compress the corresponding files
for year in ${years}; do
    tarball="${output_dir}/water_year_${year}.tar.gz"
    echo "Compressing files for water year ${year} into ${tarball}..."
    tar -czvf "${tarball}" *_agg_rounded_WR${year}.csv
done

echo "Compression completed for all years."

