HOOVER
================================================================================

Hoover is the framework we will use to recover incomplete Darshan logs from
compute nodes' ramdisks and reconstruct valid Darshan logs.

It is a three-component system that uses RabbitMQ to transport and load balance
pieces of Darshan logs.  The current architectural documentation can be found
here:

https://sites.google.com/a/lbl.gov/glennklockwood/nersc-infrastructure/rabbitmq

This directory also contains a git submodule to my fork of the amqptools repo.
To get the submodule, either pass the `-r` flag to `git clone`, or clone this
repo and then do

    git submodule init
    git submodule update

The C consumer can be activated via

    AMQP_QUEUE=logs ./amqpspawn 'darshanlogs' '' -h localhost --foreground

where we are binding a queue called `logs` to the `darshanlogs` exchange with
a null (`''`) routing key.  This null routing key in the context of `amqpspawn`
is only used as an argument to the callback executable.
