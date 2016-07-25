#!/usr/bin/env python
import os
import h5py
import time
import datetime

_H5LMT_PATH = '/global/project/projectdirs/pma/www/daily'
_H5LMT_FILE_SYSTEM =  'cori_snx11168'
_LMT_TIMESTEP = 5
_ONE_MINUTE = 60 / _LMT_TIMESTEP

def print_output():
    """
    define time ranges here
    """
    get_open_rate( '2016-07-20 20:57:17', 143.274110145 )
    get_open_rate( '2016-07-20 21:43:36', 22.810845344  )

def get_open_rate( timestamp_string, walltime_secs ):
    """
    1. opens/sec is a rate.  multiply by _LMT_TIMESTEP to get
       a aggregate opens within each 5-second window
    2. OSS gaps is the number of OSS datapoints that were
       missing at the 5-second window.  an indirect indicator
       of how lossy the data transfer might've been for the
       MDS data
    """
    f = h5py.File( os.path.join( _H5LMT_PATH, timestamp_string.split()[0], _H5LMT_FILE_SYSTEM + '.h5lmt' ), 'r' )
    dset_ops = f['MDSOpsGroup/MDSOpsDataSet']
    dset_missing = f['FSMissingGroup/FSMissingDataSet']
    open_idx = list(dset_ops.attrs['OpNames']).index('open')

    ### calculate start/stop indices
    t0 = datetime.datetime.strptime( timestamp_string, '%Y-%m-%d %H:%M:%S')
    t0_idx = ( int(time.mktime(t0.timetuple())) - f['FSStepsGroup/FSStepsDataSet'][0] ) / _LMT_TIMESTEP
    tf_idx = t0_idx + int( walltime_secs ) / _LMT_TIMESTEP

    print "%15s, %10s, %10s" % ( 'timestamp', 'opens/sec', 'OSS gaps')
    for time_idx in range(t0_idx - _ONE_MINUTE, tf_idx + _ONE_MINUTE):
        print "%15s, %10.1f, %10d" % ( f['FSStepsGroup/FSStepsDataSet'][time_idx],
              dset_ops[open_idx, time_idx],
              dset_missing[:,time_idx].sum() )

    f.close()

if __name__ == '__main__':
    print_output()
