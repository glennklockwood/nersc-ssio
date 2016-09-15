#!/usr/bin/env python

import h5py
import time
import sys

_LMT_TIMESTEP = 5
_BIN_WIDTH_SECS = 24*3600

for h5lmt in sys.argv[1:]:
    with h5py.File( h5lmt, 'r' ) as f:
        num_timesteps = f['OSTReadGroup/OSTBulkReadDataSet'].shape[1]
        istart = 0
        istop = istart + _BIN_WIDTH_SECS / _LMT_TIMESTEP

        while (istop-istart) > 1:
            print "%19s, %12.2f, %12.2f" % (
                time.strftime( "%Y-%m-%d %H:%M:%S", time.localtime( f['FSStepsGroup/FSStepsDataSet'][istart] ) ),
                f['OSTReadGroup/OSTBulkReadDataSet'][:,istart:istop].sum()/2**30,
                f['OSTWriteGroup/OSTBulkWriteDataSet'][:,istart:istop].sum()/2**30)

            istart = istop
            istop = istart + _BIN_WIDTH_SECS / _LMT_TIMESTEP
            if istop >= num_timesteps:
                istop = num_timesteps - 1


#       print "%2d %5d %5d %12.2f %12.2f %19s %19s" % (
#           istart / (_BIN_WIDTH_SECS / _LMT_TIMESTEP),
#           istart,
#           istop,
#           f['OSTReadGroup/OSTBulkReadDataSet'][:,istart:istop].sum()/2**30,
#           f['OSTWriteGroup/OSTBulkWriteDataSet'][:,istart:istop].sum()/2**30,
#           time.strftime( "%Y-%m-%d %H:%M:%S", time.localtime( f['FSStepsGroup/FSStepsDataSet'][istart] ) ),
#           time.strftime( "%Y-%m-%d %H:%M:%S", time.localtime( f['FSStepsGroup/FSStepsDataSet'][istop] ) ) )
