import os
from pathlib import Path

# Define the directory where the parquet files are located
out_dir = Path('/home/jmframe/data/AORC/camels/1980_to_2024')
log_file = out_dir / 'processing_log.txt'

# Create the processing log file if it does not exist
if not log_file.exists():
    log_file.touch()

# Get all parquet files ending with "_coverage.parquet" in the output directory
parquet_files = list(out_dir.glob('*_coverage.parquet'))

# Add basin numbers to the processing log file
with open(log_file, 'a') as file:
    for parquet_file in parquet_files:
        # Extract the basin number from the filename
        basin_number = parquet_file.stem.split('_coverage')[0]
        # Write the basin number to the log file
        file.write(f"{basin_number}: finished\n")

