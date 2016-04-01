#!/bin/bash
#
#  Identify the Lustre fs and append its stats to node-specific files.
#  Intended to be called before and after an MPI job using srun, e.g.,
#  
#      srun -ntasks-per-node=1 $PWD/drop_lustre_stats.sh
#      srun ./my_mpi_job
#      srun -ntasks-per-node=1 $PWD/drop_lustre_stats.sh
#

OUTPUT_DIR=${SLURM_SUBMIT_DIR:-$PWD}

# Try to find the Lustre file system stats file
LUSTRE_FS=${1-snx11168}
LUSTRE_PROC_STATS="$(find /proc/fs/lustre/llite -name stats 2>/dev/null | grep "$LUSTRE_FS")"
if [ -z "$LUSTRE_PROC_STATS" ]; then
    echo "$(date) - Could not find Lustre fs stats file for $LUSTRE_FS on $(hostname)"
else
    echo "$(date) - Found Lustre fs stats file at $LUSTRE_PROC_STATS"
fi

for stats_file in stats read_ahead_stats statahead_stats max_cached_mb
do
    cat $(dirname $LUSTRE_PROC_STATS)/${stats_file} >> $OUTPUT_DIR/lustre-${stats_file}.$(hostname).out
done
