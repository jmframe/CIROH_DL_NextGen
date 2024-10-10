#!/bin/bash
#SBATCH --job-name=ngen         # Job name
#SBATCH --partition=normal               # Partition (queue) name
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --nodelist=compute005
#SBATCH --ntasks=40                     # Number of tasks (processes)
#SBATCH --time=01:00:00                 # Time limit hrs:min:sec
#SBATCH --output=ngen_vpu09_%j.out   # Standard output log
#SBATCH --error=ngen_vpu09_%j.err   # Standard error log

module purge
cd "/home/jmframe/ngen/extern/lstm/config"
module load UDUNITS/2.2.28-GCCcore-12.3.0
module load netCDF/4.9.2-gompi-2023a
module load netCDF-Fortran/4.6.1-gompi-2023a
module load SQLite/3.42.0-GCCcore-12.3.0
module load OpenMPI/4.1.5-GCC-12.3.0 
source "/home/jmframe/ngen/env/bin/activate"

# Make sure to run the partitions: $HYDROFABRIC_GPKG $HYDROFABRIC_GPKG vpu09_partitions.json 16 "" ""
N_PARTITIONS=32
PARTITION_FILE="/home/jmframe/ngen/cmake_build/vpu09_partitions_32.json"

HYDROFABRIC_GPKG="/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/nextgen_09.gpkg"

# ngen is linked to cmake_build
#./ngen $HYDROFABRIC_GPKG all $HYDROFABRIC_GPKG all realization_09.json
#./ngen $HYDROFABRIC_GPKG 'cat-1487385' $HYDROFABRIC_GPKG 'tnx-1000005413' ./realization_09.json
mpirun -n $N_PARTITIONS ../ngen $HYDROFABRIC_GPKG all $HYDROFABRIC_GPKG all ../realization_09.json $PARTITION_FILE
