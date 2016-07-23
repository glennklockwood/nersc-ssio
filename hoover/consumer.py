#!/usr/bin/env python
#
#  Boilerplate code for Hoover-consumer.
#

import pika

conn = pika.BlockingConnection( pika.ConnectionParameters( host='localhost' ) )
channel = conn.channel()

channel.queue_declare(queue='logs')

channel.queue_bind( exchange='darshanlogs', queue='logs')#, routing_key='*' )

def callback( ch, method, properties, body ):
    print "Received message [%r]" % body

channel.basic_consume(callback, queue='logs', no_ack=True)

channel.start_consuming()
