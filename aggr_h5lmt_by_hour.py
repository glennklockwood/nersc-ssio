#!/usr/bin/env python
#
#  Take one or more h5lmt files and add up the bytes in/out on an hourly or
#  daily basis.
#
#  For example, to get the total bytes in and out of all Edison scratch file
#  systems in 2015, go into the h5lmt directory and do something like
#
#      aggr_h5lmt_by_hour.py 2015-*-*/edison_*.h5lmt --brief --summary
#

import datetime
import h5py
import sys
import os
import argparse

_BYTES_TO_GIB = 1.0 / 1024.0 / 1024.0 / 1024.0
_LMT_TIMESTEP = 5
_TIMESTEPS_PER_BIN = 60 * 60 / _LMT_TIMESTEP

parser = argparse.ArgumentParser(description='aggregate bytes in/out from h5lmt file every hour')
parser.add_argument('h5lmt', metavar='N', type=str, nargs='+', help='h5lmt file to examine')
parser.add_argument('--brief', dest='brief', action='store_true', help='print a single line of output per h5lmt')
parser.add_argument('--summary', dest='summary', action='store_true', help='print a summary of all output')
parser.add_argument('--bytes', dest='bytes', action='store_true', help='print bytes, not GiB')
args = parser.parse_args()

def add_h5lmt_to_aggregator( f, agg ):
    key = f['FSStepsGroup/FSStepsDataSet'].attrs['day']
    if key not in agg:
        agg[key] = { "in": 0, "out": 0 }
    agg[key]['in'] += f['OSTReadGroup/OSTBulkReadDataSet'][:,:].sum() * _LMT_TIMESTEP
    agg[key]['out'] += f['OSTWriteGroup/OSTBulkWriteDataSet'][:,:].sum() * _LMT_TIMESTEP

def print_brief_summary( agg ):
    tot_read, tot_write = 0.0, 0.0
    for day in sorted( agg.keys() ):
        sums = agg[day]
        vol_read = sums['in']
        vol_write = sums['out']
        tot_read  += vol_read
        tot_write += vol_write
        if args.bytes:
            print "%s %15ld %15ld" % ( day, vol_read, vol_write )
        else:
            print "%s %12.2f %12.2f" % ( day, vol_read * _BYTES_TO_GIB, vol_write * _BYTES_TO_GIB )

    return tot_read, tot_write

def print_hourly_summary( f ):
    if args.bytes:
        print "%3s %5s %5s %15s %15s" % ( 'hr', 'i0', 'if', 'bytes Read', 'bytes Written' )
    else:
        print "%3s %5s %5s %12s %12s" % ( 'hr', 'i0', 'if', 'GiB Read', 'GiB Written' )

    tot_read, tot_write = 0.0, 0.0
    for timestep in range( f['FSStepsGroup/FSStepsDataSet'].shape[0] / _TIMESTEPS_PER_BIN ):
        istart = timestep * _TIMESTEPS_PER_BIN  # inclusive
        istop  = istart + _TIMESTEPS_PER_BIN    # exclusive

        vol_read  = f['OSTReadGroup/OSTBulkReadDataSet'][:,istart:istop].sum() * _LMT_TIMESTEP
        vol_write = f['OSTWriteGroup/OSTBulkWriteDataSet'][:,istart:istop].sum() * _LMT_TIMESTEP
        tot_read  += vol_read
        tot_write += vol_write
        if args.bytes:
            print "%3d %5d %5d %15ld %15ld %s %s" % (
                timestep,
                istart,
                istop,
                vol_read,
                vol_write,
                datetime.datetime.fromtimestamp( f['FSStepsGroup/FSStepsDataSet'][istart] ).strftime( "%Y-%m-%d %H:%M:%S" ),
                datetime.datetime.fromtimestamp( f['FSStepsGroup/FSStepsDataSet'][istop] ).strftime( "%Y-%m-%d %H:%M:%S" )
                )
        else:
            print "%3d %5d %5d %12.2f %12.2f %s %s" % (
                timestep,
                istart,
                istop,
                vol_read*_BYTES_TO_GIB,
                vol_write*_BYTES_TO_GIB,
                datetime.datetime.fromtimestamp( f['FSStepsGroup/FSStepsDataSet'][istart] ).strftime( "%Y-%m-%d %H:%M:%S" ),
                datetime.datetime.fromtimestamp( f['FSStepsGroup/FSStepsDataSet'][istop] ).strftime( "%Y-%m-%d %H:%M:%S" )
                )
   
    return tot_read, tot_write

if __name__ == '__main__':
    agg = {}
    tot_read, tot_write = 0.0, 0.0
    n = 0
    for h5lmt in args.h5lmt:
        filepath = os.path.abspath( h5lmt )
        try:
            with h5py.File( filepath, 'r' ) as f:
                if args.brief:
                    add_h5lmt_to_aggregator( f, agg )
                else:
                    a, b = print_hourly_summary( f )
                    tot_read += a
                    tot_write += b
                    n += 1
        except IOError:
            continue

    if args.brief:
        tot_read, tot_write = print_brief_summary( agg )
        n = len( agg )

    if args.summary:
        if args.bytes:
            print "\nTotal read:  %15ld bytes (avg %15.2f bytes/day)" % ( tot_read, tot_read / n )
            print "Total write: %15ld bytes (avg %15.2f bytes/day)" % ( tot_write, tot_write / n )
        else:
            print "\nTotal read:  %12.2f GiB (avg %12.2f GiB/day)" % ( tot_read * _BYTES_TO_GIB, tot_read / n * _BYTES_TO_GIB )
            print "Total write: %12.2f GiB (avg %12.2f GiB/day)" % ( tot_write * _BYTES_TO_GIB, tot_write / n * _BYTES_TO_GIB )
