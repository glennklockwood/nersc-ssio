#!/bin/bash
#
#  Identify the DataWarp mount and append its DVS mount and IPC stats to a
#  node-specific file.  Intended to be called before and after an MPI job
#  using srun, e.g.,
#  
#      srun -ntasks-per-node=1 $PWD/drop_bb_stats.sh $SLURM_SUBMIT_DIR
#      srun ./my_mpi_job
#      srun -ntasks-per-node=1 $PWD/drop_bb_stats.sh $SLURM_SUBMIT_DIR
#
#   The resulting files can then be processed using the included 
#   diff_concatenated_dvs_stats.py script.
#

OUTPUT_DIR="${1:-$SLURM_SUBMIT_DIR}"

DW_DIR="${DW_JOB_STRIPED:-$DW_JOB_PRIVATE}"

if [ -z "$DW_DIR" ]; then
    echo "$(date) - DW_JOB_{STRIPED,PRIVATE} are NULL" >&2
    exit 1
fi

### Try to find the DataWarp file system stats file
DVS_PROC_STATS=$(mount | grep $(sed -e 's#/$##g' <<< $DW_DIR) | grep -o 'nodefile=[^,]*,' | cut -d= -f2 | sed -e's/,$//' -e's/nodenames/stats/')
if [ -z "$DVS_PROC_STATS" ]; then
    echo "$(date) - Could not find DataWarp/DVS fs stats file on $(hostname)" >&2
    exit 1
fi

cat $DVS_PROC_STATS >> $OUTPUT_DIR/mount-stats.$(hostname).out
cat /proc/fs/dvs/ipc/stats >> $OUTPUT_DIR/ipc-stats.$(hostname).out
