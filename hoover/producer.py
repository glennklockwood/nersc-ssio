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
import hashlib

_HOOVER_EXCHANGE = 'darshanlogs'

def send_message( channel, message, exchange=_HOOVER_EXCHANGE, routing_key='' ):
    channel.exchange_declare( exchange=exchange, type='direct' )
    print "Sending message to exchange [%s] with routing_key [%s]" % ( exchange, routing_key )
    return channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties( delivery_mode=2 ))

def hoover( glob_str ):
    filenames = glob.glob( glob_str )

    conn = pika.BlockingConnection( pika.ConnectionParameters( 'localhost' ) )
    channel = conn.channel()
    send_message( channel, build_manifest( filenames ), routing_key='logs' )

    conn.close()
        

def build_manifest( filenames ):
    manifest = []
    for filename in filenames:
        manifest.append( {
            'filename': filename,
            'size':     os.path.getsize(filename),
            'sha1':     sha1sum( filename )
        } )
    return json.dumps( manifest )

def sha1sum( filename, blocksize=2**30 ):
    hasher = hashlib.new('sha1')
    with open(filename, 'rb') as f:
        buf = f.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(blocksize)
    return hasher.hexdigest()

if __name__ == '__main__':
    hoover( sys.argv[1] )
