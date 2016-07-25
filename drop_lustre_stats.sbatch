#!/bin/bash
#SBATCH -J test-ior
#SBATCH -N 64
#SBATCH -p debug
#SBATCH -t 00:30:00
#SBATCH -o job.out
#SBATCH -e job.err

BIN=/global/u2/g/glock/ssio/bin
COUNTER_OUTPUT_DIR=$SLURM_SUBMIT_DIR
LUSTRE_FS=snx11168

### Drop initial counter values 
srun -n 64 --ntasks-per-node=1 $BIN/drop_lustre_stats.sh $COUNTER_OUTPUT_DIR $LUSTRE_FS

### Perform I/O against the DataWarp file system
cd $SCRATCH
srun -n 2048 $BIN/ior -s 8 -t 128M -b 128M -v -e -C -F

### Drop final counter values
srun -n 64 --ntasks-per-node=1 $BIN/drop_lustre_stats.sh $COUNTER_OUTPUT_DIR $LUSTRE_FS

### Package up the per-node counter files
cd $COUNTER_OUTPUT_DIR
for counters in lustre-max_cached_mb lustre-read_ahead_stats lustre-statahead_stats lustre-stats; do
    tar --remove-files -czf ${counters}.tgz ${counters}*.out
done
