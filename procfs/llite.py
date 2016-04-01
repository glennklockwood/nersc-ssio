#!/usr/bin/env python

def parse_concatenated_llite_stats( fp ):
    """
    For a file that contains the following Lustre llite stats concatenated:
      - /proc/fs/lustre/llite/fs/stats
      - /proc/fs/lustre/llite/fs/read_ahead_stats
    walk the file and report deltas when a key is encountered more than once.
    - We call one dump of /proc/fs/lustre/llite/.../stats a "page"
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
        key, value_string = _extract_kv( line )
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

def parse_llite_stats( fp ):
    """
    Convert a single page of Lustre llite stats counters into a dict
    """
    data = {}

    for line in fp:
        k, v = _extract_kv( line )
        data[k] = []
        for element in v.strip().split():
            if '.' in element:
                data[k].append( float(element) )
            else:
                data[k].append( int(element) )

    return data

def _extract_kv( line ):
    tokens = line.split()
    key, value = None, None
    for i, tok in enumerate(tokens):
        if tok.startswith('['):
            value = tokens[i - 2]
            key = '_'.join(tokens[0:i-2])
            break
    if key is None:
        key, value = tokens[0:2] 

    return key, value

if __name__ == '__main__':
    main()
