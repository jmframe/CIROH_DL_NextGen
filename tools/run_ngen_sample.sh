#!/bin/bash

# Set the BASINID
BASINID=3297032

echo "Running NextGen for BASIN ID: $BASINID"

# List of catchment IDs
catchment_list=(
  "cat-3297023" "cat-3297024" "cat-3297025" "cat-3297026"
  "cat-3297027" "cat-3297028" "cat-3297029" "cat-3297030"
  "cat-3297031" "cat-3297032" "cat-3297081" "cat-3297082"
  "cat-3297085" "cat-3297086" "cat-3297087" "cat-3297103"
  "cat-3297109" "cat-3297110" "cat-3297111" "cat-3297112"
  "cat-3297129" "cat-3297132" "cat-3297133" "cat-3297148"
  "cat-3297149" "cat-3297150" "cat-3297151" "cat-3297152"
  "cat-3297153" "cat-3297162" "cat-3297163" "cat-3297164"
  "cat-3297183" "cat-3297184" "cat-3297185" "cat-3297186"
  "cat-3297187" "cat-3297188" "cat-3297189" "cat-3297190"
)

# Loop through each catchment ID and run the command
for catchment in "${catchment_list[@]}"; do
  echo "Processing $catchment"


  ./ngen/cmake_build/ngen \
    "/home/shared/uageology/data/2024_si_ngen/wb-$BASINID/config/catchments.geojson" "$catchment" \
    "/home/shared/uageology/data/2024_si_ngen/wb-$BASINID/config/nexus.geojson" "$catchment" \
    "/home/shared/uageology/data/2024_si_ngen/wb-$BASINID/config/realization.json" \
    "/home/shared/uageology/data/2024_si_ngen/wb-$BASINID/config/partitions.json"
  
  # Check if the command failed
  if [ $? -ne 0 ]; then
    echo "Failed to process $catchment"
    echo "$catchment" >> failed_catchments.txt
  fi
done

