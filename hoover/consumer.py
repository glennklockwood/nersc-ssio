#!/usr/bin/env python
#
#  Boilerplate code for Hoover-consumer.
#

from __future__ import print_function

import os
import sys
import json
import pika
import hoover
import argparse
import StringIO

_HOOVER_EXCHANGE = 'darshanlogs'
_HOOVER_OUTPUT_DIR = os.path.join(os.getcwd(), "out")
_HOOVER_LOG_QUEUE = 'logs'
_DEFAULT_VERBOSITY = 'INFO'

def begin_consume( host, exchange, output_dir, port=pika.connection.ConnectionParameters.DEFAULT_PORT ):
    def receive_message( channel, method, properties, body ):
        """
        Callback function when an incoming message is received.  Relies on
        `output_dir` being in scope, so we define this callback within the
        consumer function
        """
        if properties.headers is None or 'checksum' not in properties.headers:
            ### Messages without checksums at all are useless to us; discard
            print("No checksum provided in message header")
            print("Message header = [%s]" % json.dumps(properties.headers))
            return -1
        elif 'filename' not in properties.headers:
            ### No filename with checksum indicates a manifest being sent.
            ### Key manifests their expected contents so that if a manifest
            ### has to be re-sent, it is not duplicated on the consumer side
            output_file = os.path.basename('manifest_%s.json' % properties.headers['checksum'])
        else:
            ### Actual files have intended file names embedded
            output_file = os.path.basename(properties.headers['filename'])
        output_file = os.path.join(output_dir, output_file)

        ### Write the message body into the intended file
        with open( output_file, 'w+' ) as fp:
            fp.write(body)

        ### Calculate checksum and compare to manifest
        checksum = hoover.checksum( StringIO.StringIO(body) )
        if checksum == properties.headers['checksum']:
            print("Wrote output to %s (cksum: %s)" % (output_file, checksum)) 
        else:
            print("Checksum mismatch for %s (cksum: %s, was expecting %s)" % 
                (output_file, checksum, properties.headers['checksum']))

    conn = pika.BlockingConnection( pika.ConnectionParameters( host=host, port=port ) )

    channel = conn.channel()

    channel.exchange_declare( exchange=_HOOVER_EXCHANGE, type='direct' )

    channel.queue_declare(queue=_HOOVER_LOG_QUEUE)

    channel.queue_bind(exchange=_HOOVER_EXCHANGE, queue=_HOOVER_LOG_QUEUE)

    channel.basic_consume(receive_message, queue=_HOOVER_LOG_QUEUE, no_ack=True)

    print("Subscribed to exchange [%s] on %s:%d" % 
        (exchange, host, port))
    channel.start_consuming()

if __name__ == '__main__':
    parser = argparse.ArgumentParser( add_help=False )
    parser.add_argument("-e", "--exchange",
                        type=str,
                        default=_HOOVER_EXCHANGE,
                        help="name of exchange to consume")
    parser.add_argument("-o", "--output",
                        type=str,
                        default=_HOOVER_OUTPUT_DIR,
                        help="output directory")
    parser.add_argument("-h", "--host",
                        type=str,
                        default="localhost",
                        help="RabbitMQ host")
    parser.add_argument("-p", "--port",
                        type=int,
                        help="RabbitMQ port")
    parser.add_argument("-?", "--help",
                        action="store_true",
                        help="show this help message")
    parser.add_argument("-v", "--verbose",
                        action="count",
                        help="verbosity level; repeat up to three times")

    args = parser.parse_args()

    if args.help:
        parser.print_help()
        sys.exit(1)

    if not os.path.isdir( args.output ):
        print("Output directory %s doesn't exist; creating it" % args.output)
        os.makedirs( args.output )

    if args.port:
        begin_consume( host=args.host, exchange=args.exchange, output_dir=args.output, port=args.port )
    else:
        begin_consume( host=args.host, exchange=args.exchange, output_dir=args.output )
