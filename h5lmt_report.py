#!/usr/bin/env python

import h5py
import numpy as np
import pandas

import os
import sys
import glob
import datetime

_FILE_SYSTEMS = ( 'hopper_scratch', 'hopper_scratch2', 'edison_snx11025',
                  'edison_snx11035', 'edison_snx11036', 'cori_snx11168' )
_H5LMT_BASE_DIR = '/global/project/projectdirs/pma/www/daily' 

def missing_data( f ):
    dset_missing = f['FSMissingGroup/FSMissingDataSet'][:,:]
    dset_ost = f['OSTReadGroup/OSTBulkReadDataSet'][:,:] + f['OSTWriteGroup/OSTBulkWriteDataSet'][:,:]

    total_loss = 0
    total_missing = 0
    probably_total_loss = 0
    for i in range( dset_missing.shape[1] ):
        missing = dset_missing[:,i].sum()
        total_missing += missing
        if missing == dset_missing.shape[0]:
            total_loss += 1
        elif dset_ost[:,i].sum() == 0:
            probably_total_loss += 1

    pct_missing = float(total_missing) / (dset_missing.shape[0]*dset_missing.shape[1])
    pct_total_loss = float(total_loss) / dset_missing.shape[1]
    pct_probably_total_loss = float(probably_total_loss) / dset_missing.shape[1]
    # print "Missing data points:     %5d (%.2f%%)" % ( total_missing, 100.0 * pct_missing )
    # print "Totally lost time steps: %5d (%.2f%%)" % ( total_loss, 100.0 * pct_total_loss )
    # print "Prob total lost:         %5d (%.2f%%)" % ( probably_total_loss, 100.0 * pct_probably_total_loss )
    return pct_missing, pct_total_loss, pct_probably_total_loss

def count_mds_ops( f ):
    dset_data = f['MDSOpsGroup/MDSOpsDataSet'][:,:]
    output = {}
    for i in range( dset_data.shape[0] ):
        opname = f['MDSOpsGroup/MDSOpsDataSet'].attrs['OpNames'][i]
        opsum = dset_data[i,:].sum() * 5.0
        if opsum > 0:
            output[ opname ] = opsum
    return output

def avg_mds_cpu( f ):
    return f['MDSCPUGroup/MDSCPUDataSet'][:].sum() / f['MDSCPUGroup/MDSCPUDataSet'].shape[0]

def avg_oss_cpu( f ):
    return f['OSSCPUGroup/OSSCPUDataSet'][:,:].sum() / f['OSSCPUGroup/OSSCPUDataSet'].shape[0] / f['OSSCPUGroup/OSSCPUDataSet'].shape[1]

def sum_read_gbs( f ):
    return f['OSTReadGroup/OSTBulkReadDataSet'][:,:].sum() * 5.0 / 2**30

def sum_write_gbs( f ):
    return f['OSTWriteGroup/OSTBulkWriteDataSet'][:,:].sum() * 5.0 / 2**30

def lmt_date(f):
    return f['FSStepsGroup/FSStepsDataSet'].attrs['day']

def lmt_fs(f):
    return f['FSStepsGroup/FSStepsDataSet'].attrs['fs']

def h5lmt_to_array( filename ):
    with h5py.File(filename, 'r') as f:
        mds_ops = count_mds_ops( f )
        uncertainty = missing_data(f)
        output = {
#           'date':         lmt_date(f),
#           'fs':           lmt_fs(f),
            'sum closes':       mds_ops.get('close', 0.0),
            'sum opens':        mds_ops.get('open', 0.0),
            'sum unlinks':      mds_ops.get('unlink', 0.0),
            'sum getattrs':     mds_ops.get('getattr', 0.0),
            'sum getxattrs':    mds_ops.get('getxattr', 0.0),
            'sum setattrs':     mds_ops.get('setattr', 0.0),
            'avg mds pcpu': avg_mds_cpu(f),
            'avg oss pcpu': avg_oss_cpu(f),
            'gb read':      sum_read_gbs(f),
            'gb written':   sum_write_gbs(f),
            'pct missing':  uncertainty[0],
            'pct total loss': uncertainty[1],
            'pct probable total loss': uncertainty[2],
        }
    return output
    
if  __name__ == '__main__':
   
    if len(sys.argv) < 3:
        sys.stderr.write("syntax: %s start_date end_date, where dates are in YYYY-MM-DD format\n")
        sys.exit(1)

    t_start = datetime.date( *[ int(x) for x in sys.argv[1].split('-') ] )
    t_stop  = datetime.date( *[ int(x) for x in sys.argv[2].split('-') ] )

    print "%s to %s" % (t_start, t_stop )

    duds = []
    t = t_start
    fs_data = { 'all': {} }
    while t <= t_stop:
        date_str = t.strftime( "%Y-%m-%d" )
        for fs in _FILE_SYSTEMS:
            filename = os.path.join( _H5LMT_BASE_DIR, t.strftime("%Y-%m-%d"), fs + '.h5lmt' )
            if os.path.isfile(filename):
                print "Processing %s" % filename
            else:
                print "Skipping %s (not found)" % filename
                continue

            try:
                data = h5lmt_to_array( filename )
                if fs not in fs_data:
                    fs_data[fs] = { date_str : data }
                else:
                    fs_data[fs][date_str] = data
            ### capture corrupt h5lmt files
            except IOError:
                duds.append( filename )

        ### add this data to the aggregated 'all' statistic set
        for fs in fs_data.keys():
            if date_str not in fs_data[fs].keys():
                continue
            if date_str not in fs_data['all']:
                fs_data['all'][date_str] = {}
                
            for key in fs_data[fs][date_str].keys():
                if key not in ( 'date', 'fs' ):
                    if key not in fs_data['all'][date_str]:
                        fs_data['all'][date_str][key] = fs_data[fs][date_str][key]
                    else:
                        fs_data['all'][date_str][key] += fs_data[fs][date_str][key]

        t += datetime.timedelta(days=1)

    for fs, data in fs_data.iteritems():
        df = pandas.DataFrame.from_dict( data, orient='index' )
        df.index.name = 'date'
        df[sorted(df.keys())].to_csv( fs + '.csv' )
        

    print duds
