#!/bin/bash
#SBATCH -J llapi-perf
#SBATCH -N 256
#SBATCH -n 6144
#SBATCH -p debug
#SBATCH -t 30:00
#SBATCH -o llapi-perf.out
#SBATCH -e llapi-perf.err
#
#  Check below for the definition of TARGET in the innermost loop.  Ensure that
#  these target files exist before submitting this job.  This script was devised
#  for Edison, which had 24 cores per node and a 96-OST file system.  Salt to
#  taste.
#

BIN="$PWD/llapi-perf"

for nodes in 256 128 64 32 16 8 4 2 1
do
    for ppn in 1 2 12 24
    do
        for stripes in 1 2 4 8 16 32 64 96
        do
            TARGET="$SCRATCH/stripe${stripes}/random.bin"
            NPROCS=$(($ppn * $nodes))
            echo "$(date) :: Running nodes=$nodes ppn=$ppn nprocs=$NPROCS stripes=$stripes"
            srun -N $nodes -n $NPROCS $BIN -i "$TARGET"
            echo "$(date) :: Finished running"
        done
    done
done
