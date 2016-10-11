#!/usr/bin/env python

import StringIO
import json

# Need to mask out unknown threads because the DVS IPC stats file contains
# random garbage after each Instance block
_VALID_INSTANCE_KEYS = [
    'Total Threads',
    'Created Threads',
    'Active Threads',
    'Idle Threads',
    'Blocked Threads',
    'Thread Limit',
    'Total Queues',
    'Active Queues',
    'Free Queues',
    'Queued Messages',
]

def parse_concatenated_dvs_mount_stats( fp ):
    """
    For a file that contains a series of concatenated /proc/fs/dvs/stats
    outputs, walk the file and report deltas when a key is encountered more
    than once.
    - We call one dump of /proc/fs/dvs/mounts/.../stats a "page"
    - A page is comprised of key-value pairs, where the key is a counter, and
      the value is a series of counts
    - This script operates on a file that contains a series of pages
    - When a key is detected a second (or third, etc) time, we assume it belongs
      to a different page from the previous page on which it was detected
    - We assume pages are not interleaved
    """

    key_counts = {}
    this_page_num = 1
    this_page = {}
    counter_pages = []
    for line in fp:
        ### digest the line
        key, value_string = line.split(':', 1)
        values = []
        if key in key_counts: # not the first time seeing this counter
            key_counts[key] += 1
            if key_counts[key] > this_page_num: # new page detected
                this_page_num += 1
                counter_pages.append( this_page )
                this_page = {}
        else: # first time seeing this counter
           key_counts[key] = 1

        ### cast counter values into ints or floats
        for value in value_string.strip().split():
            if '.' in value:
                values.append( float( value ) )
            else:
                values.append( int( value ) )

        ### Add this counter to the page currently being built
        this_page[key] = values

    ### append last page being populated
    counter_pages.append( this_page )

    return counter_pages

def parse_dvs_mount_stats( fp ):
    """
    Convert a single page of DVS mount stats counters into a dict
    """
    data = {}

    for line in fp:
        k, v = line.split(':')
        data[k] = []
        for element in v.strip().split():
            if '.' in element:
                data[k].append( float(element) )
            else:
                data[k].append( int(element) )

    return data


def parse_concatenated_dvs_ipc_stats( fp ):
    """
    Read in a file containing concatenated DVS IPC stats and split it into an
    array of counters.  Right now only consider IPC counters; discard the others
    """
    counter_pages = []
    page_txt = None
    for line in fp:
        if line.startswith('DVS IPC Transport Statistics'):
            if page_txt is None:
                page_txt = StringIO.StringIO()
            else:
                page_txt.flush()
                page_txt.seek(0, 0)
                values = parse_dvs_ipc_stats(page_txt)
                counter_pages.append(values['ipc_counters'])
                page_txt.close()
                page_txt = StringIO.StringIO()
        page_txt.write(line)
    ### flush last page
    page_txt.flush()
    page_txt.seek(0, 0)
    values = parse_dvs_ipc_stats(page_txt)
    counter_pages.append(values['ipc_counters'])

    return counter_pages

def parse_dvs_ipc_stats( fp ):
    # num  logical state
    # 1    parsing dvs stats file, looking for "DVS IPC Transport" header
    # 2    parsing dvs ipc file, looking for "Refill Stats:" header
    # 3    parsing refill stats, looking for "Instance \d:" header.  THIS WILL BREAK IF THERE IS NO INSTANCE HEADER
    # 4    parsing instance stats on valid keys only, looking for "Size Distributions"
    # 5    parsing size distributions

    state = 1
    this_instance = 'default'

    data = {}
    for line in fp:
        if state == 1:
            if line.startswith('DVS IPC Transport Statistics'):
                state += 1
                data['ipc_counters'] = {}
            else:
                parse_dvs_stats_line( line, data['counters'] )
        elif state == 2:
            if line.startswith('Refill Stats:'):
                data['ipc_refill_stats'] = []
                state += 1
            else:
                k, v = line.strip().rsplit(None, 1)
                data['ipc_counters'][k] = [ int(v.strip(), 16) ]
        elif state == 3:
            if line.startswith('Instance') or line.strip() == "":
                state += 1
                if line.strip() != "":
                    this_instance = line.rsplit(None, 1)[-1].strip(': \n')
                data['ipc_instances'] = {}
                data['ipc_instances'][this_instance] = {}
            else:
                data['ipc_refill_stats'] += [ int(x,16) for x in line.strip().split() ]
        elif state == 4:
            if line.startswith("Size Distributions"):
                state += 1
            elif line.startswith('Instance'):
                this_instance = line.rsplit(None, 1)[-1].strip(': \n')
                data['ipc_instances'][this_instance] = {}
            else:
                # handle all the garbage that can appear after an Instance: block
                try:
                    k, v = line.strip().rsplit(None, 1)
                except ValueError:
                    pass
                else:
                    if k in _VALID_INSTANCE_KEYS:
                        data['ipc_instances'][this_instance][k] = int(v.strip(),16)
    if len(data.keys()) == 0:
        raise Exception("parsed null page")

    return data

if __name__ == '__main__':
    main()
