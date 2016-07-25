#!/bin/bash
#SBATCH -J test-ior
#SBATCH -N 64
#SBATCH -p debug
#SBATCH -t 00:30:00
#SBATCH -o job.out
#SBATCH -e job.err
#DW jobdw type=scratch access_mode=striped capacity=3999GiB

BIN=/global/u2/g/glock/ssio/bin
COUNTER_OUTPUT_DIR=$SLURM_SUBMIT_DIR

### Drop initial counter values 
srun -n 64 --ntasks-per-node=1 $BIN/drop_bb_stats.sh $COUNTER_OUTPUT_DIR

### Perform I/O against the DataWarp file system
cd $DW_JOB_STRIPED
srun -n 2048 $BIN/ior -s 8 -t 128M -b 128M -v -e -C -F

### Drop final counter values
srun -n 64 --ntasks-per-node=1 $BIN/drop_bb_stats.sh $COUNTER_OUTPUT_DIR

### Package up the per-node counter files
cd $COUNTER_OUTPUT_DIR
tar --remove-files -czf mount-stats.tgz mount-stats.*.out
tar --remove-files -czf ipc-stats.tgz ipc-stats.*.out
