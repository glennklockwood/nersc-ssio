#!/usr/bin/env python
#
# Given an h5lmt file, extract a certain subset of time and convert it to csv
#
# On NERSC machines, be sure to `module load h5py` to get h5py and pandas
#

import datetime
import h5py
import sys
import os
import pandas

_DELTA_TIME = 5
_TARGET_DATASETS = [
    ( 'OSTReadGroup/OSTBulkReadDataSet',   'ost-read.csv' ),
    ( 'OSTWriteGroup/OSTBulkWriteDataSet', 'ost-write.csv' ),
]

f = h5py.File( sys.argv[1], 'r' )
idx_0 = (int(sys.argv[2]) % 86400) / _DELTA_TIME
idx_f = (int(sys.argv[3]) % 86400) / _DELTA_TIME + 1 # add one to catch the last bin
t_startofday = int(int(sys.argv[2]) / 86400) * 86400

timestamps = [ (t_startofday + _DELTA_TIME * t) for t in range( idx_0, idx_f + 1 ) ]

for dataset, outfile in _TARGET_DATASETS:
    df = pandas.DataFrame( None, index=timestamps )
    df.index.name = 'timestamp'
    row_id = 0
    for one_row in f[dataset]:
        record = one_row[idx_0:idx_f+1]
        label = "%s%d" % ( outfile.split('-',1)[0], row_id )
        df[label] = record
        row_id += 1
    df.to_csv( outfile )
