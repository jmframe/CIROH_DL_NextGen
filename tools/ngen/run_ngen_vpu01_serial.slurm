#!/bin/bash
#SBATCH --job-name=ngen         # Job name
#SBATCH --partition=normal               # Partition (queue) name
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --nodelist=compute005
#SBATCH --ntasks=40                     # Number of tasks (processes)
#SBATCH --time=47:00:00                 # Time limit hrs:min:sec
#SBATCH --output=ngenvpu01_%j.out   # Standard output log
#SBATCH --error=ngenvpu01_%j.err   # Standard error log

module purge
cd "/home/jmframe/ngen/extern/lstm/config"
module load UDUNITS/2.2.28-GCCcore-12.3.0
module load netCDF/4.9.2-gompi-2023a
module load netCDF-Fortran/4.6.1-gompi-2023a
module load SQLite/3.42.0-GCCcore-12.3.0
source "/home/jmframe/ngen/env/bin/activate"

HYDROFABRIC_GPKG="/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/nextgen_01.gpkg"

# ngen is linked to cmake_build
#./ngen $HYDROFABRIC_GPKG all $HYDROFABRIC_GPKG all realization_09.json
#./ngen $HYDROFABRIC_GPKG 'cat-1487385' $HYDROFABRIC_GPKG 'tnx-1000005413' ./realization_09.json
../ngen $HYDROFABRIC_GPKG all $HYDROFABRIC_GPKG all ../realization_01.json