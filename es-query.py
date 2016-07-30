#!/usr/bin/env python
#
#  Boilerplate code to demonstrate how to interact with ElasticSearch and
#  understand the structure of its output.
#
#  Glenn K. Lockwood, Lawrence Berkeley National Laboratory     June 2016
#

import json
import StringIO
import pycurl

_INDEX = 'cori-collectd-*'
_QUERY = 'hostname:bb* AND plugin:disk AND plugin_instance:nvme*'
_LIMIT_OUTPUT = 10
_API_ENDPOINT = 'http://localhost:9200/%s/_search' % _INDEX

_QUERY_DICT = {
    "size": _LIMIT_OUTPUT,
    "query": {
        "query_string": {
            "query": _QUERY,
            "analyze_wildcard": True
        }
    }
}

def print_disk_records():
    """
    Demonstrates how to parse the records produced by the collectd 'disk' module
    """
    output_dict = query_es( _QUERY_DICT )

    hits_list = output_dict['hits']['hits']

    for hit in hits_list:
        hit_src = hit['_source']
        if hit_src['plugin'] == 'disk':
            if hit_src['collectd_type'] in ( 'disk_ops', 'disk_merged', 'disk_octets', 'disk_time' ):
                value_str = "read=%f write=%f" % (hit_src['read'], hit_src['write'])
            elif hit_src['collectd_type'] == 'pending_operations':
                value_str = "value=%f" % hit_src['value']
            elif hit_src['collectd_type'] == 'disk_io_time':
                value_str = "disk_io_time=%f" % hit_src['io_time']
            else:
                value_str = "(unknown collectd_type)"
                print hit_src

            print "host=%-8s device=%-8s %-20s %s" % ( 
                hit_src['hostname'],
                hit_src['plugin_instance'],
                hit_src['collectd_type'],
                value_str )

def query_es( query_dict ):
    """
    Convert an input dict describing an ElasticSearch query into json, POST
    it to the ES cluster, then convert the response json into a dict
    """
    out_buffer = StringIO.StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, _API_ENDPOINT)
    c.setopt(c.WRITEFUNCTION, out_buffer.write)
    c.setopt(c.POST, 1)
    c.setopt(c.POSTFIELDS, json.dumps(query_dict))
    c.perform()
    c.close()
    return json.loads( out_buffer.getvalue() )


if __name__ == '__main__':
    print_disk_records()
