#!/usr/bin/env python

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

def parse_dvs_mount_stats( fp ):
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
                data['ipc_counters'][k] = int(v.strip())
        elif state == 3:
            if line.startswith('Instance'):
                state += 1
                this_instance = line.rsplit(None, 1)[-1].strip(': \n')
                data['ipc_instances'] = {}
                data['ipc_instances'][this_instance] = {}
            else:
                data['ipc_refill_stats'] += [ int(x) for x in line.strip().split() ]
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
                        data['ipc_instances'][this_instance][k] = int(v.strip())
    return data

if __name__ == '__main__':
    main()
