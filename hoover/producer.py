#!/usr/bin/env python
#
#  Proof-of-concept code to demonstrate how Hoover-producer will transport
#  partial Darshan logs off of compute nodes.
#

import os
import sys
import glob
import json
import pika
import StringIO
import hoover

_HOOVER_EXCHANGE = 'darshanlogs'

def send_message( channel, body, exchange=_HOOVER_EXCHANGE, routing_key='', headers={} ):
    channel.exchange_declare( exchange=exchange, type='direct' )
    print "Sending message to exchange [%s] with routing_key [%s]" % ( exchange, routing_key )
    return channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(headers=headers, delivery_mode=2 ))

def produce( glob_str ):
    filenames = glob.glob( glob_str )

    manifest = build_manifest( filenames )
    manifest_json = json.dumps(manifest)
    manifest_cksum = hoover.checksum( StringIO.StringIO(manifest_json) )

    conn = pika.BlockingConnection( pika.ConnectionParameters( 'localhost' ) )
    channel = conn.channel()

    send_message( 
        channel=channel,
        body=manifest_json,
        routing_key='logs',
        headers={ 'checksum': manifest_cksum })

    for manifest_entry in manifest:
        # TODO: add failsafe in case message can't be opened...
        with open( manifest_entry['filename'], 'r' ) as fp:
            send_message( 
                channel=channel,
                body=fp.read(),
                routing_key='logs',
                headers=manifest_entry )

    conn.close()

def build_manifest( filenames ):
    manifest = []
    for filename in filenames:
        manifest.append( {
            'filename': filename,
            'size':     os.path.getsize(filename),
            'checksum': hoover.checksum_file( filename )
        } )
    return manifest

if __name__ == '__main__':
    produce( sys.argv[1] )
