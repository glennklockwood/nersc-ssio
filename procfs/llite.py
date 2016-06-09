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
        key, values = _extract_kv( line )
        if key in key_counts: # not the first time seeing this counter
            key_counts[key] += 1
            if key_counts[key] > this_page_num: # new page detected
                this_page_num += 1
                counter_pages.append( this_page )
                this_page = {}
        else: # first time seeing this counter
           key_counts[key] = 1

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
        data[k] = v

    return data

def _extract_kv( line ):
    """
    Lustre does not use a consistent format for its procfs counters
    interface.  We must deal with the following possibilities:

    ## stats
    snapshot_time             1465429958.452678 secs.usecs
    read_bytes                4560934621 samples [bytes] 1 2147479552 1957691429756371
    write_bytes               3364345392 samples [bytes] 1 2147479552 157804380246588

    ## read_ahead_stats
    hit max r-a issue         12622218315 samples [pages]
    failed to reach end       12684129648 samples [pages]

    ## statahead_stats
    statahead total: 2152972
    statahead wrong: 20440
    agl total: 2152972

    ## max_cached_mb
    users: 248
    max_cached_mb: 258209
    used_mb: 242069
    """
    tokens = line.strip().split()
    key = None
    values = []
    for i, tok in enumerate(tokens):
        ### deal with `read_ahead_stats` which has no simple key-value delimiter
        ### also takes care of *most* of `stats`
        if tok.startswith('['):
            values = tokens[i - 2:]
            key = '_'.join(tokens[0:i-2])
            break
    if key is None:
        key, values = ( tokens[0].rstrip(':'), tokens[1:] )

    output_values = []
    for value in values:
        try:
            if '.' in value:
                output_values.append( float( value ) )
            else:
                output_values.append( int( value ) )
        except ValueError:
            pass

    return key, output_values

if __name__ == '__main__':
    main()
