#!/bin/bash
#SBATCH --job-name=ngen         # Job name
#SBATCH --partition=normal               # Partition (queue) name
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --nodelist=compute005
#SBATCH --ntasks=40                     # Number of tasks (processes)
#SBATCH --time=47:00:00                 # Time limit hrs:min:sec
#SBATCH --output=01187300_%j.out   # Standard output log
#SBATCH --error=01187300_%j.err   # Standard error log

module purge
cd "/home/jmframe/ngen/extern/lstm/config"
module load UDUNITS/2.2.28-GCCcore-12.3.0
module load netCDF/4.9.2-gompi-2023a
module load netCDF-Fortran/4.6.1-gompi-2023a
module load SQLite/3.42.0-GCCcore-12.3.0
source "/home/jmframe/ngen/env/bin/activate"

HYDROFABRIC_GPKG="/home/jmframe/ngen/extern/lstm/hydrofabric/v20.1/gpkg/nextgen_01.gpkg"

CATS="cat-13677,cat-13678,cat-13681,cat-13680,cat-13664,cat-13679,cat-13663,cat-13662"
NEXS="nex-13663,nex-13662,nex-13664,nex-13665"

../ngen_serial_quiet $HYDROFABRIC_GPKG $CATS $HYDROFABRIC_GPKG $NEXS ../realization_01187300.json
