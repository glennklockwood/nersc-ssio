#!/usr/bin/env python

import procfs.dvs as dvs
import sys
import json

with open( sys.argv[1], 'r' ) fp:
    print json.dumps( dvs.parse_dvs_mount_stats( fp ), indent=4, sort_keys=True )
