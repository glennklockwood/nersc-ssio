#!/usr/bin/env python
#
#  Boilerplate code for Hoover-consumer.
#

import os
import sys
import json
import uuid
import pika
import hoover
import argparse
import StringIO

_HOOVER_EXCHANGE = 'darshanlogs'
_HOOVER_OUTPUT_DIR = os.path.join(os.getcwd(), "out")
_HOOVER_LOG_QUEUE = 'logs'

def begin_consume( host, exchange, output_dir, port=pika.connection.ConnectionParameters.DEFAULT_PORT ):
    def receive_message( channel, method, properties, body ):
        output_file = os.path.join( output_dir,
            os.path.basename(properties.headers.get('filename', 'manifest_%s.json' % uuid.uuid4().hex)))
        with open( output_file, 'w+' ) as fp:
            fp.write(body)
        print "Wrote output to %s" % output_file

        ### calculate checksum and compare to manifest
        checksum = hoover.checksum( StringIO.StringIO(body) )

        if 'checksum' not in properties.headers:
            print "No checksum provided in message header!"
        elif checksum != properties.headers['checksum']:
            print "Checksum does NOT match!"
        else:
            print "Checksum matches!"

    conn = pika.BlockingConnection( pika.ConnectionParameters( host=host, port=port ) )
    channel = conn.channel()

    channel.exchange_declare( exchange=_HOOVER_EXCHANGE, type='direct' )

    channel.queue_declare(queue=_HOOVER_LOG_QUEUE)

    channel.queue_bind(exchange=_HOOVER_EXCHANGE, queue=_HOOVER_LOG_QUEUE)

    channel.basic_consume(receive_message, queue=_HOOVER_LOG_QUEUE, no_ack=True)

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

    args = parser.parse_args()

    if args.help:
        parser.print_help()
        sys.exit(1)

    if not os.path.isdir( args.output ):
        print "Output directory %s doesn't exist; creating it" % args.output
        os.makedirs( args.output )

    if 'port' in args:
        begin_consume( host=args.host, exchange=args.exchange, output_dir=args.output, port=args.port )
    else:
        begin_consume( host=args.host, exchange=args.exchange, output_dir=args.output )
