#!/usr/bin/env python

import os
import sys
import time
import datetime
import argparse
import cPickle as pickle
import MySQLdb
import h5py
import numpy as np

_LMT_TIMESTEP = 5.0

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_MYSQL_FETCHMANY_LIMIT = 1000

_QUERY_OST_DATA = """
SELECT
    UNIX_TIMESTAMP(TIMESTAMP_INFO.`TIMESTAMP`) as ts,
    OST_NAME as ostname,
    READ_BYTES as read_b,
    WRITE_BYTES as write_b
FROM
    OST_DATA
INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = OST_DATA.TS_ID
INNER JOIN OST_INFO ON OST_INFO.OST_ID = OST_DATA.OST_ID
WHERE
    TIMESTAMP_INFO.`TIMESTAMP` >= ADDTIME('%s', '%.1f')
AND TIMESTAMP_INFO.`TIMESTAMP` < ADDTIME('%s', '%1.f')
ORDER BY ts, ostname;
""" % ( '%s', -1.5 * _LMT_TIMESTEP, '%s', _LMT_TIMESTEP / 2.0 )

### Find the most recent timestamp for each OST before a given time range.  This
### is to calculate the first row of diffs for a time range.  There is an
### implicit assumption that there will be at least one valid data point for
### each OST in the 24 hours preceding t_start.  If this is not the case, not
### every OST will be represented in the output of this query.
_QUERY_FIRST_OST_DATA = """
SELECT
    OST_INFO.OST_NAME,
    UNIX_TIMESTAMP(TIMESTAMP_INFO.`TIMESTAMP`),
    OST_DATA.READ_BYTES,
    OST_DATA.WRITE_BYTES
FROM
    (
        SELECT
            OST_DATA.OST_ID AS ostid,
            MAX(OST_DATA.TS_ID) AS newest_tsid
        FROM
            OST_DATA
        INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = OST_DATA.TS_ID
        WHERE
            TIMESTAMP_INFO.`TIMESTAMP` < '%s'
        AND TIMESTAMP_INFO.`TIMESTAMP` > ADDTIME(
            '%s',
            '-24:00:00.0'
        )
        GROUP BY
            OST_DATA.OST_ID
    ) AS last_ostids
INNER JOIN OST_DATA ON last_ostids.newest_tsid = OST_DATA.TS_ID AND last_ostids.ostid = OST_DATA.OST_ID
INNER JOIN OST_INFO on OST_INFO.OST_ID = last_ostids.ostid
INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = last_ostids.newest_tsid
"""

_QUERY_TIMESTAMP_MAPPING = """
SELECT
    UNIX_TIMESTAMP(`TIMESTAMP`)
FROM
    TIMESTAMP_INFO
WHERE
    `TIMESTAMP` >= '%s'
AND `TIMESTAMP` < '%s'
ORDER BY
    TS_ID
"""

class LMTDB(object):
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None, pickles=None):
        self.pickles = pickles
        if pickles is None:
            if dbhost is None:
                dbhost = os.environ.get('PYLMT_HOST')
            if dbuser is None:
                dbuser = os.environ.get('PYLMT_USER')
            if dbpassword is None:
                dbpassword = os.environ.get('PYLMT_PASSWORD')
            if dbname is None:
                dbname = os.environ.get('PYLMT_DB')
            self.db = MySQLdb.connect( 
                host=dbhost, 
                user=dbuser, 
                passwd=dbpassword, 
                db=dbname)
        else:
            self.db = None
    def __enter__(self):
        return self
    def __die__(self):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')
    def __exit__(self, exc_type, exc_value, traceback):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def close( self ):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def get_ost_data( self, t_start, t_stop ):
        """
        Wrapper for backend-specific data dumpers that retrieve all counter data
        between two timestamps
        """
        if self.pickles is not None:
            return self._ost_data_from_pickle( t_start, t_stop )
        else:
            return self._ost_data_from_mysql( t_start, t_stop )

    def get_timestamp_map( self, t_start, t_stop ):
        """
        Get the timestamps associated with a t_start/t_stop from LMT
        """
        query_str = _QUERY_TIMESTAMP_MAPPING % (
            t_start.strftime( _DATE_FMT ), 
            t_stop.strftime( _DATE_FMT ) 
        )
        return self._query_mysql( query_str )

    def _ost_data_from_mysql( self, t_start, t_stop ):
        """
        Get read/write bytes data from LMT between [ t_start, t_stop )
        """
        query_str = _QUERY_OST_DATA % ( 
            t_start.strftime( _DATE_FMT ), 
            t_stop.strftime( _DATE_FMT ) 
        )
        return self._query_mysql( query_str )

    def _query_mysql( self, query_str ):
        """
        Generator function that connects to MySQL, runs a query, and buffers output
        """
        cursor = self.db.cursor()
        cursor.execute( query_str )

        while True:
            rows = cursor.fetchmany(_MYSQL_FETCHMANY_LIMIT)
            if rows == ():
                break
            for row in rows:
                yield row

    def _ost_data_from_pickle( self, t_start, t_stop ):
        """
        Generator function that reads the output of a previous MySQL query from a
        pickle file
        """
        pickle_file = "ost_data.%d-%d.pickle" % (
            int(time.mktime( t_start.timetuple() )),
            int(time.mktime( t_stop.timetuple() )) )
        with open( pickle_file, 'r' ) as fp:
            from_pickle = pickle.load( fp )

        for row in from_pickle:
            yield row

def _pickle_ost_data( lmtdb, t_start, t_stop ):
    """
    Retrieve and pickle a set of OST data
    """
    to_pickle = []
    for row in lmtdb.get_ost_data( t_start, t_stop ):
        to_pickle.append( row )
    pickle_file = "ost_data.%d-%d.pickle" % (
        int(time.mktime( t_start.timetuple() )),
        int(time.mktime( t_stop.timetuple() )) )
    with open( pickle_file, 'w' ) as fp:
        pickle.dump( to_pickle, fp )

def _get_index_from_time( t ):
    return (t.hour * 3600 + t.minute * 60 + t.second) / int(_LMT_TIMESTEP)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument( 'tstart', type=str, help="lower bound of time to scan, in YYY-mm-dd HH:MM:SS format" )
    parser.add_argument( 'tstop', type=str, help="upper bound of time to scan, in YYY-mm-dd HH:MM:SS format" )
    parser.add_argument( '--read-pickle',  action='store_true', help="attempt to read from pickle instead of MySQL" )
    parser.add_argument( '--write-pickle', action='store_true', help="write output to pickle" )
    args = parser.parse_args()
    if not ( args.tstart and args.tstop ):
        parser.print_help()
        sys.exit(1)
    if args.read_pickle and args.write_pickle:
        sys.stderr.write("--read-pickle and --write-pickle are mutually exclusive\n" )
        sys.exit(1)

    try:
        t_start = datetime.datetime.strptime( args.tstart, _DATE_FMT )
        t_stop = datetime.datetime.strptime( args.tstop, _DATE_FMT )
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT );
        raise

    if args.read_pickle:
        lmtdb = LMTDB( pickles=True )
    else:
        lmtdb = LMTDB()

    ### initialize hdf5 file
    ost_ct = 248
    ts_ct = 86400/int(_LMT_TIMESTEP)
    h5f = h5py.File('testfile.hdf5', 'w')
    if 'OSTReadGroup/OSTBulkReadDataSet' not in h5f:
        h5f.create_dataset('OSTReadGroup/OSTBulkReadDataSet', (ost_ct, ts_ct), dtype='f8')
    if 'OSTWriteGroup/OSTBulkWriteDataSet' not in h5f:
        h5f.create_dataset('OSTWriteGroup/OSTBulkWriteDataSet', (ost_ct, ts_ct), dtype='f8')
    if 'FSStepsGroup/FSStepsDataSet' not in h5f:
        h5f.create_dataset('FSStepsGroup/FSStepsDataSet', (ts_ct,), dtype='i8')

    ### initialize hdf5 timestamps in given range.  must track the actual
    ### indices observed because there may be timestamps that are entirely
    ### missing from the LMT DB
    ts_map = np.empty( shape=(ts_ct,), dtype='i8' )
    min_ts_idx = None
    max_ts_idx = None
    for tup_out in lmtdb.get_timestamp_map( t_start, t_stop ):
        ts_idx = _get_index_from_time( datetime.datetime.fromtimestamp( tup_out[0] ) )
        ts_map[ ts_idx  ] = tup_out[0]
        if min_ts_idx is None or ts_idx < min_ts_idx:
            min_ts_idx = ts_idx
        if max_ts_idx is None or ts_idx > max_ts_idx:
            max_ts_idx = ts_idx
    h5f['FSStepsGroup/FSStepsDataSet'][min_ts_idx:max_ts_idx] = ts_map[min_ts_idx:max_ts_idx]

    ### populate read/write bytes data
    ost_names = []
    prev_data = {}
    if args.write_pickle:
        lmtdb._ost_data_to_pickle( t_start, t_stop )
    else:
        for tup_out in lmtdb.get_ost_data( t_start, t_stop ):
            timestamp = datetime.datetime.fromtimestamp( tup_out[0] )
            ost_name = tup_out[1]
            read_bytes = tup_out[2]
            write_bytes = tup_out[3]
            ts_idx = _get_index_from_time(timestamp)

            if ost_name not in ost_names:
                ost_idx = len(ost_names)
                ost_names.append( tup_out[1] )
            else:
                ost_idx = ost_names.index( ost_name )

            if ost_name in prev_data:
                h5f['OSTReadGroup/OSTBulkReadDataSet'][ost_idx, ts_idx] = read_bytes - prev_data[ost_name]['read']
                h5f['OSTWriteGroup/OSTBulkWriteDataSet'][ost_idx, ts_idx] = write_bytes - prev_data[ost_name]['write']

            if ost_name not in prev_data:
                prev_data[ost_name] = { }
            prev_data[ost_name]['read'] = read_bytes
            prev_data[ost_name]['write'] = write_bytes

    h5f.close()
    lmtdb.close()