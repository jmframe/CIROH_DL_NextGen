#!/bin/bash

# Function to load a module and check if successful
load_module() {
    module load "$1" &>/dev/null
    if [[ $? -eq 0 ]]; then
        echo "Loaded module $1 successfully."
    else
        echo "Module $1 cannot be loaded, it might be unavailable or misspelled."
        module avail "$1"
        exit 1
    fi
}

# Load required modules
load_module GCCcore/12.2.0
load_module netCDF-Fortran/4.6.1-gompi-2023a
#LIBRARY_PATH="/apps/GCCcore/12.3.0/lib/gcc/x86_64-pc-linux-gnu/12.3.0/:$LIBRARY_PATH"

pushd /home/jmframe/ngen/extern/t-route

env NETCDF="/apps/netCDF/4.9.2-gompi-2023a/include" ./compiler.sh no-e

popd

