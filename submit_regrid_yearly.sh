#!/bin/bash
#SBATCH --job-name=regrid_yearly
#SBATCH --account=m4986
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=40
#SBATCH --exclusive
#SBATCH --time=00:45:00
#SBATCH --output=/pscratch/sd/c/chriscox/regriddingtests/logs/regrid_yearly_%j.out
#SBATCH --error=/pscratch/sd/c/chriscox/regriddingtests/logs/regrid_yearly_%j.err
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=christopher.cox@student.nmt.edu

module load conda
conda activate regridding

cd /global/u1/c/chriscox/software/python/netcdf_tools

python regrid_yearly_parallel.py
