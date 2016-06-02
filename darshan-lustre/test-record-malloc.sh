#!/bin/bash
#
#  Run the test program through Valgrind to expose memory leaks and buffer
#  overflows on a variety of different file locations and geometries
#
set -x
valgrind --tool=memcheck \
         --leak-check=yes \
         --show-reachable=yes \
         --num-callers=20 \
         --track-fds=yes \
         --read-var-info=yes \
         ./test-record-malloc \
         $SCRATCH/stripe_small/4 \
         $SCRATCH/stripe_large/1tb.bin \
         $SCRATCH/stripe_small/1 \
         $SCRATCH/stripe_medium/1 \
         $SCRATCH/stripe_small/2 \
         $SCRATCH/stripe_small/3 \
         $HOME/.bashrc \
         $SCRATCH/random.bin
set +x
