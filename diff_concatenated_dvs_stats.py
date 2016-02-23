#!/usr/bin/env python
#
#  For a file that contains a series of /proc/fs/dvs/stats outputs, walk the
#  file and report deltas when a key is encountered more than once
#

# - We call one dump of /proc/fs/dvs/mounts/.../stats a "page"
# - A page is comprised of key-value pairs, where the key is a counter, and the
#   value is a series of counts
# - This script operates on a file that contains a series of pages
# - When a key is detected a second (or third, etc) time, we assume it belongs to
#   a different page from the previous page on which it was detected
# - We assume pages are not interleaved

import procfs.dvs as dvs
import sys
import json

def diff_single_dvs_stats_file( fp ):
    key_counts = {}
    this_page_num = 1
    these_counters = {}
    counter_diffs = []
    for line in fp:
        ### digest the line
        key, value_string = line.split(':', 1)
        values = []
        if key in key_counts: # not the first time seeing this counter
            key_counts[key] += 1
            if key_counts[key] > this_page_num: # new page detected
                this_page_num += 1
                counter_diffs.append( these_counters )
                these_counters = {}
        else: # first time seeing this counter
           key_counts[key] = 1

        ### cast counter values into ints or floats
        for value in value_string.strip().split():
            if '.' in value:
                values.append( float( value ) )
            else:
                values.append( int( value ) )

        ### calculate diff if key has been previously encountered
        if len(counter_diffs) > 0:
            prev_counter_diffs = counter_diffs[-1]
            if key in prev_counter_diffs:
                value_diffs = []
                changes = 0.0
                for idx, value in enumerate(values):
                    delta = value - prev_counter_diffs[key][idx]
                    value_diffs.append( delta )
                    changes += abs( delta )
                if changes != 0: # only record changes in counter values
                    these_counters[key] = value_diffs
            else: # a new key has appeared in this page
                these_counters[key] = values
        else:
            these_counters[key] = values

    ### append last page being populated
    counter_diffs.append( these_counters )

    return counter_diffs[1:]

if __name__ == '__main__':
    with open( sys.argv[1], 'r' ) as fp:
        print json.dumps( diff_single_dvs_stats_file( fp ), indent=4, sort_keys=True )

