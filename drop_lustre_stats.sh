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

# Try to find the Lustre file system stats file
LUSTRE_FS=${2-snx11168}
LUSTRE_PROC_STATS="$(find /proc/fs/lustre/llite -name stats 2>/dev/null | grep "$LUSTRE_FS")"
if [ -z "$LUSTRE_PROC_STATS" ]; then
    echo "$(date) - Could not find Lustre fs stats file for $LUSTRE_FS on $(hostname)"
fi

for stats_file in stats read_ahead_stats statahead_stats max_cached_mb
do
    cat $(dirname $LUSTRE_PROC_STATS)/${stats_file} >> $OUTPUT_DIR/lustre-${stats_file}.$(hostname).out
done
