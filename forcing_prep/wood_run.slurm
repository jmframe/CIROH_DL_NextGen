#!/bin/bash
#SBATCH --job-name=gage_loop         # Job name
#SBATCH --partition=normal             # Partition (queue) name
#SBATCH --nodes=1                      # Number of nodes
#SBATCH --ntasks=40                    # Number of tasks (processes)
#SBATCH --time=48:00:00                 # Time limit hrs:min:sec
#SBATCH --output=wood_%j.log    # Standard output and error log

source /home/jmframe/CIROH_DL_NextGen/forcing_prep/venv/bin/activate 
pip install fiona
cd /home/jmframe/CIROH_DL_NextGen/forcing_prep

# Ensure the generate script is executable
chmod +x generate_camels_lumped.py

python3 generate_camels_lumped.py "wood_config_aorc.yaml"

deactivate

