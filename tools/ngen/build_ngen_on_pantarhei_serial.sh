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
load_module cmake
load_module UDUNITS/2.2.28-GCCcore-12.2.0
load_module netCDF/4.9.2

# Ensure the user is in the correct directory
cd "$(dirname "$0")/../ngen" || exit

# Ask user to update submodules
read -p "Update submodules (yes/no)? " update_submodules
if [[ "$update_submodules" == "yes" ]]; then
    git submodule update --init --recursive
else
    echo "Skipping submodule update."
fi

# Create build directory and configure the build
mkdir -p cmake_build
cd cmake_build || exit

# Set NetCDF paths
NETCDF_ROOT="/apps/netCDF/4.9.2-iimpi-2023a"
export CPATH="$NETCDF_ROOT/include:$CPATH"
export LIBRARY_PATH="$NETCDF_ROOT/lib:$LIBRARY_PATH"
export LD_LIBRARY_PATH="$NETCDF_ROOT/lib:$LD_LIBRARY_PATH"

cmake \
    -DNetCDF_ROOT=$NETCDF_ROOT \
    -DNGEN_WITH_MPI:BOOL=OFF \
    -DNGEN_WITH_NETCDF:BOOL=ON \
    -DNGEN_WITH_SQLITE:BOOL=OFF \
    -DNGEN_WITH_UDUNITS:BOOL=ON \
    -DNGEN_WITH_BMI_FORTRAN:BOOL=OFF \
    -DNGEN_WITH_BMI_C:BOOL=ON \
    -DNGEN_WITH_PYTHON:BOOL=ON \
    -DNGEN_WITH_ROUTING:BOOL=ON \
    -DNGEN_WITH_TESTS:BOOL=OFF \
    -DNGEN_QUIET:BOOL=OFF \
    -S ..

# Build ngen
cmake --build . --target ngen -- -j "$(nproc)"

echo "Build completed"

cmake \
    -DNetCDF_ROOT=$NETCDF_ROOT \
    -DNGEN_WITH_MPI:BOOL=OFF \
    -DNGEN_WITH_NETCDF:BOOL=ON \
    -DNGEN_WITH_SQLITE:BOOL=OFF \
    -DNGEN_WITH_UDUNITS:BOOL=ON \
    -DNGEN_WITH_BMI_FORTRAN:BOOL=OFF \
    -DNGEN_WITH_BMI_C:BOOL=ON \
    -DNGEN_WITH_PYTHON:BOOL=ON \
    -DNGEN_WITH_ROUTING:BOOL=ON \
    -DNGEN_WITH_TESTS:BOOL=ON \
    -DNGEN_QUIET:BOOL=OFF \
    -S ..


cmake --build . --target test_unit -- -j $(nproc)
./test/test_unit

echo "Testing complete"

