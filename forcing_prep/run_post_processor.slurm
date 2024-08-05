#!/bin/bash
#SBATCH --job-name=post_process         # Job name
#SBATCH --partition=normal             # Partition (queue) name
#SBATCH --nodes=1                      # Number of nodes
#SBATCH --ntasks=40                    # Number of tasks (processes)
#SBATCH --time=48:00:00                 # Time limit hrs:min:sec
#SBATCH --output=post_processor%j.log    # Standard output and error log

cd /home/jmframe/CIROH_DL_NextGen/forcing_prep

python3 -u post_process.py
