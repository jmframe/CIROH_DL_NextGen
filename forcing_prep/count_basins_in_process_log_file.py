# Read the processing_log.txt file
#with open('/home/jmframe/data/AORC/camels/1980_to_2024/processing_log.txt', 'r') as file:
#/home/jmframe/data/CAMELS_US/wood_july2024/1980_to_2024/
with open('/home/jmframe/data/CAMELS_US/wood_july2024/1980_to_2024/processing_log.txt', 'r') as file:
    lines = file.readlines()

# Extract basin IDs and remove duplicates
basin_ids = set(line.split(':')[0] for line in lines)

# Count the unique basin IDs
unique_basin_count = len(basin_ids)

print(f"Number of unique basin IDs: {unique_basin_count}")

