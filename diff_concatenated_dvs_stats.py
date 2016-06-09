#!/usr/bin/env python
#
#  Given a list of files named according to "mount-stats.nidXXXXX.out" which
#  contain the output of /proc/fs/dvs/mounts/.../stats before and after some
#  event for a node `nidXXXXX`, compare every counter and report a table of
#  counters that changed on each node.
#

import re
import sys
import tarfile
import warnings

import pandas
import procfs.dvs
import procfs.llite

def diff_single_stats_file( fp, fstype ):
    """
    For a file that contains a series of concatenated /proc/fs/dvs/stats
    outputs, identify counters that change between consecutive dumps
    - We call one dump of /proc/fs/dvs/mounts/.../stats a "page"
    - A page is comprised of key-value pairs, where the key is a counter, and
      the value is a series of counts
    - This script operates on a file that contains a series of pages
    - When a key is detected a second (or third, etc) time, we assume it belongs
      to a different page from the previous page on which it was detected
    - We assume pages are not interleaved
    """

    if fstype == "dvs":
        page_list = procfs.dvs.parse_concatenated_dvs_mount_stats(fp)
    elif fstype == "llite":
        page_list = procfs.llite.parse_concatenated_llite_stats(fp)
    counter_diffs = []

    ### obtain a list of all observed counters
    all_counters = set([])
    for page in page_list:
        all_counters = all_counters | set(page.keys())

    if len( page_list ) > 1:
        # compare every page to the one before it and calculate changes
        for idx in range( 1, len(page_list) ):
            a = page_list[idx-1]
            b = page_list[idx]
            page_diff = {}
            for key in all_counters:
                if key not in a and key in b:
                    val_a = [ 0 for x in b[key] ]
                    val_b = b[key]
                elif key not in b and key in a:
                    val_a = a[key]
                    val_b = [ 0 for x in a[key] ]
                else:
                    val_a = a[key]
                    val_b = b[key]
                page_diff[key] = [ val_b[i] - val_a[i] for i in range( len(val_a) ) ]
            total_change = 0.0
            for key in page_diff.keys():
                total_change += sum( [ abs( x ) for x in page_diff[key] ] )
            if total_change > 0.0:
                counter_diffs.append( page_diff )
    elif len(page_list) == 1:
        raise Exception( "Only one page of counters detected" )
    else:
        raise Exception( "No counters detected" )

    return counter_diffs

def ingest_counter_data_filenames( filenames, fstype ):
    """
    given a list of file names, build a super structure of counter diffs
    per node
    """
    all_counter_data = {}
    for filename in filenames:
        rex_match = re.search( '(nid\d+)', filename )
        if rex_match:
            nodename = rex_match.group(0)
        else:
            i = 0
            while "unknown%d" % i in all_counter_data:
                i += 1
            nodename = "unknown%d" % i
            warnings.warn( "unable to detect node name from %s; using %s" % (filename, nodename) )
        with open( filename, 'r' ) as fp:
            ### only keep first diff since our files contain only one diff
            all_counter_data[ nodename ] = diff_single_stats_file( fp, fstype )[0] 
    return all_counter_data

def ingest_counter_data_tgz( tgzfile, fstype ):
    """
    given a tar file, build a super structure of counter diffs per node
    """
    filenames = []
    tar = tarfile.open( tgzfile )
    for tarinfo in tar:
        if tarinfo.isreg():
            filenames.append( tarinfo.name )

    all_counter_data = {}
    for filename in filenames:
        rex_match = re.search( '(nid\d+)', filename )
        if rex_match:
            nodename = rex_match.group(0)
        else:
            i = 0
            while "unknown%d" % i in all_counter_data:
                i += 1
            nodename = "unknown%d" % i
            warnings.warn( "unable to detect node name from %s; using %s" % (filename, nodename) )
        ### only keep first diff since our files contain only one diff
        all_counter_data[ nodename ] = diff_single_stats_file( tar.extractfile( filename ), fstype )[0] 

    tar.close()
    return all_counter_data

if __name__ == '__main__':
    ### assume we're parsing dvs counters by default
    if "lustre" in sys.argv[0]:
        fstype="llite"
    else:
        fstype="dvs"

    if sys.argv[1].endswith( 'tgz' ) or sys.argv[1].endswith( '.tar.gz' ) or sys.argv[1].endswith( '.tar' ):
        all_counter_data = ingest_counter_data_tgz( sys.argv[1], fstype )
    else:
        all_counter_data = ingest_counter_data_filenames( sys.argv[1:], fstype )
    all_counters = set([])
    for nodename in all_counter_data.keys():
        all_counters = all_counters | set(all_counter_data[ nodename ].keys())
    
    ### Because *_min_max counters encode two useful numbers, we should split
    ### them into two key/value pairs before normalizing.  First loop over
    ### counters whose name end in "_min_max"
    for counter in [ x for x in frozenset(all_counters) if x.endswith('_min_max') ]:
        ### then loop over subset of node list whose nodes contain *_min_max counters
        for node in [ x for x in all_counter_data if counter in all_counter_data[x] ]:
            base_key = counter.split('_')[0:-2]
            min_key = "_".join(base_key) + "_min"
            max_key = "_".join(base_key) + "_max"
            # retain list type so that downstream processing can handle it
            all_counter_data[node][min_key] = [ all_counter_data[node][counter][0] ]
            all_counter_data[node][max_key] = [ all_counter_data[node][counter][1] ]
            all_counters.discard(counter)
            all_counters.add(min_key)
            all_counters.add(max_key)

    ### {read,write}_bytes and osc_{read_write} encode four useful counters
    key_suffixes = [ '_ops', '_min_size', '_max_size', '_total_bytes' ]
    for counter in ( 'read_bytes', 'write_bytes', 'osc_read', 'osc_write' ):
        for node in [ x for x in all_counter_data if counter in all_counter_data[x] ]:
            for i, key_suffix in enumerate(key_suffixes):
                new_key = counter + key_suffix
                all_counter_data[node][new_key] = [ all_counter_data[node][counter][i] ]
                all_counters.add(new_key)
        all_counters.discard(counter)

    ### Normalize the data so that each node-counter pair has exactly one value.
    ### Also implicitly assume all counter values are integers; floating point
    ### counters will cause type mismatches within the dataframe column
    for node, counter_data in all_counter_data.iteritems():
        for counter in all_counters:
            if counter not in counter_data:
                all_counter_data[node][counter] = 0
            else:
                all_counter_data[node][counter] = all_counter_data[node][counter][0]

    ### Create an ordered list of all unique nodes and all unique counters for
    ### normalization
    all_nodes_list = sorted( all_counter_data.keys() )
    all_counters_list = sorted( all_counters )

    ### Build a dataframe of the data
    df = pandas.DataFrame( None, index=all_nodes_list )
    df.index.name = "node"
    for counter in all_counters_list:
        column = [ all_counter_data[x][counter] for x in all_nodes_list ]
        ### drop all columns that contain nothing but zeroes
        if sum([ abs(x) for x in column]) != 0:
            df[counter] = column

    df.to_csv( '/dev/stdout' )
