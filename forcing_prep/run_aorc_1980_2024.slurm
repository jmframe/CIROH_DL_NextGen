#!/bin/bash
#SBATCH --job-name=aorc_1980_2000         # Job name
#SBATCH --partition=normal             # Partition (queue) name
#SBATCH --nodes=1                      # Number of nodes
#SBATCH --ntasks=40                    # Number of tasks (processes)
#SBATCH --time=48:00:00                 # Time limit hrs:min:sec
#SBATCH --output=aorc_%j.log    # Standard output and error log

source /home/jmframe/CIROH_DL_NextGen/forcing_prep/venv/bin/activate 
cd /home/jmframe/CIROH_DL_NextGen/forcing_prep

# Ensure the generate script is executable
chmod +x generate.py

python3 generate.py "config_aorc_1980_2024.yaml"

deactivate

