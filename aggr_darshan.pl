#!/usr/bin/env perl
#
#  Takes the output of darshan-parser and collects the total bytes read/written
#  per file system.  Intended to have a series of darshan logs piped into it to
#  get the aggregate data motion observed by Darshan for a time period, e.g.,
#
#  #!/bin/bash
#  for i in /scratch1/scratchdirs/darshanlogs/2016/6/4/*.darshan.gz
#  do 
#      darshan-parser $i
#  done | ./darshan-extractor.pl
#
#  Glenn K. Lockwood, Lawrence Berkeley National Laboratory            June 2016
#

use strict;
use warnings;

my %data;

while ( my $line = <> ) {
    next if $line =~ m/^\s*(#|\s*$)/;
    my ( $rank, $hash, $key, $val, $fname, $fs, $fstype ) = split(m/\s+/, $line);
    if ( $fstype eq "lustre" && $key =~ m/^CP_BYTES_(\S+)/ ) {
        $data{$fs}{lc($1)} += $val;
    }
}

printf( "%12s %15s %15s\n", "FS", "Written", "Read" );
foreach my $key ( keys(%data) ) {
    printf( "%12s %15ld %15ld\n", $key, $data{$key}{'written'}, $data{$key}{'read'} );
}
