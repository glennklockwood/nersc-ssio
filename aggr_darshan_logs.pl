#!/usr/bin/env perl
#
#  Uses darshan-parser across a series of darshan logs to collect the total
#  bytes read/written per file system.   Usage:
#
#    ./aggr_darshan_logs.pl /scratch1/scratchdirs/darshanlogs/2016/6/14/*.darshan.gz
#
#  Glenn K. Lockwood, Lawrence Berkeley National Laboratory            June 2016
#

use strict;
use warnings;

my %data;
my $count = 0;

foreach my $filename ( @ARGV ) {
    if ( -e $filename ) {
        foreach my $line ( `darshan-parser $filename` ) {
            next if $line =~ m/^\s*(#|\s*$)/;
            my ( $rank, $hash, $key, $val, $fname, $fs, $fstype ) = split(m/\s+/, $line);
            ### bytes in/out
            if ( $key =~ m/^CP_BYTES_(\S+)/ ) {
                $data{$fs}{lc($1)} += $val;
            }
        }
    }
    else {
        warn "Couldn't find $filename";
    }
    $count++;
    if ( $count % 100 ) {
        printf( "." );
    }

}
printf( "\n" ) if $count > 100;

printf( "%12s %15s %15s\n", "FS", "Written", "Read" );
foreach my $key ( keys(%data) ) {
    printf( "%12s %15ld %15ld\n", $key, $data{$key}{'written'}, $data{$key}{'read'} );
}
