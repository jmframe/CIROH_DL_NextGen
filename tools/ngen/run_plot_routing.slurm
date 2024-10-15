#!/bin/bash
#SBATCH --job-name=plot_routing
#SBATCH --output=plot_routing_%j.out
#SBATCH --error=plot_routing_%j.err
#SBATCH --nodelist=compute004  # Adjust as needed
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=01:00:00  # Adjust as needed
#SBATCH --partition=normal

# Load the Python environment
source /home/jmframe/ngen/env/bin/activate

# Run the Python script
srun python3 plot_routing_vpu.py