#!/bin/bash
#SBATCH --job-name=plot_vpus_parallel
#SBATCH --output=plot_vpus_%j.out
#SBATCH --error=plot_vpus_%j.err
#SBATCH --nodelist=compute003
#SBATCH --nodes=1
#SBATCH --ntasks=20  
#SBATCH --time=04:00:00  # Adjust time as needed
#SBATCH --partition=normal

# Load the necessary Python environment
source /home/jmframe/ngen/env/bin/activate

# Run the plotting script
srun python3 -u plot_nexus_vpu.py