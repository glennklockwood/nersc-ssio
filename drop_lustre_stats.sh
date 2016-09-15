#!/bin/bash
#
#  Identify the Lustre fs and append its stats to node-specific files.
#  Intended to be called before and after an MPI job using srun, e.g.,
#  
#      srun -ntasks-per-node=1 $PWD/drop_lustre_stats.sh $SLURM_SUBMIT_DIR snx11168
#      srun ./my_mpi_job
#      srun -ntasks-per-node=1 $PWD/drop_lustre_stats.sh $SLURM_SUBMIT_DIR snx11168
#
#   The resulting files can then be processed using the included 
#   diff_concatenated_lustre_stats.py script.
#

OUTPUT_DIR="${1:-$SLURM_SUBMIT_DIR}"
LUSTRE_FS=${2-snx11168}

### Try to find the Lustre file system stats file.  File systems are sometimes
### mounted twice, so we look for the mount that contains the most stats.   The
### degenerate mounts will typically only have one or two counters.
###
### Also note that this will break if file system names have spaces in them.
max_counters=0
for candidate in $(find /proc/fs/lustre/llite -name stats 2>/dev/null | grep "$LUSTRE_FS"); do
    num_counters=$(wc -l < "$candidate")
    if [ $num_counters -gt $max_counters ]; then
        max_counters=$num_counters
        LUSTRE_PROC_STATS=$candidate
    fi
done

if [ -z "$LUSTRE_PROC_STATS" ]; then
    echo "$(date) - Could not find Lustre fs stats file for $LUSTRE_FS on $(hostname)"
fi

for stats_file in stats read_ahead_stats statahead_stats max_cached_mb
do
    cat $(dirname $LUSTRE_PROC_STATS)/${stats_file} >> $OUTPUT_DIR/lustre-${stats_file}.$(hostname).out
done
