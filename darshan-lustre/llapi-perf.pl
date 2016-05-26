#!/usr/bin/env perl
#
#  Run on the stdout of the llapi-perf.sbatch script included in this directory
#

use strict;
use warnings;

# Tue May 17 17:57:51 PDT 2016 :: Running nodes=16 ppn=2 nprocs=32 stripes=96
# opt_file: /scratch1/scratchdirs/glock/stripe2/random.bin, opt_create: 0, opt_fstat: 0, opt_lseek: 0, opt_realpath: 0, nprocs: 192, time: 28.001070 ms

my ( $nodes, $ppn, $nprocs, $stripes, $time );

while ( my $line = <> ) {
    if ( $line =~ m/Running nodes=(\d+) ppn=(\d+) nprocs=(\d+) stripes=(\d+)/ ) {
        ( $nodes, $ppn, $nprocs, $stripes ) = ( $1, $2, $3, $4 );
    }
    elsif ( $line =~ m/^opt_file:.*time: (\S+) ms/ ) {
        printf( "%5d %4d %5d %5d %.6f\n",
            $nodes, $ppn, $nprocs, $stripes, $1 );
    }
}
