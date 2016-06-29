#!/usr/bin/env python
#
#  Uses darshan-parser across a series of darshan logs to collect the total
#  bytes read/written per file system.   Usage:
#
#    ./aggr_darshan_logs.py /scratch1/scratchdirs/darshanlogs/2016/6/14/*.darshan.gz
#
#  Glenn K. Lockwood, Lawrence Berkeley National Laboratory            June 2016
#
import os
import sys
import subprocess


# my %data;
data = {}
# my $count = 0;
count = 0

# foreach my $filename ( @ARGV ) {
for filename in sys.argv[1:]:
#     if ( -e $filename ) {
    if os.path.isfile( filename ):
#         foreach my $line ( `darshan-parser $filename` ) {
        p = subprocess.Popen( [ 'darshan-parser', filename ], stdout=subprocess.PIPE )
        for line in iter(p.stdout.readline, b''):
#             next if $line =~ m/^\s*(#|\s*$)/;
            if line.startswith('#') or len(line.strip()) == 0:
                continue
#             my ( $rank, $hash, $key, $val, $fname, $fs, $fstype ) = split(m/\s+/, $line);
            args = line.split()
            key = args[2]
            val = args[3]
            fs  = args[5]
            if len(args) < 7:
                print "[%s]" % line.strip()
                continue
#             if ( $key =~ m/^CP_BYTES_(\S+)/ ) {
#                 $data{$fs}{lc($1)} += $val;
            if key.startswith('CP_BYTES_'):
                if fs not in data:
                    data[fs] = { 'read': 0, 'written': 0 }
                if key[9:].startswith('WRITTEN'):
                    data[fs]['written'] += int(val)
                elif key[9:].startswith('READ'):
                    data[fs]['read'] += int(val)
#             }
#         }
#     }
#     else {
    else:
        warnings.warn("Couldn't find %s" % filename)
#         warn "Couldn't find $filename";
#     }
#     $count++;
    count += 1
#     if ( $count % 100 ) {
    if count % 100 == 0:
#         printf( "." );
        sys.stdout.write( "." )
#     }
# 
# }
# printf( "\n" ) if $count > 100;
if count > 100:
    sys.stdout.write("\n")

# printf( "%12s %15s %15s\n", "FS", "Written", "Read" );
print "%12s %15s %15s" % ( "FS", "Written", "Read" )
# foreach my $key ( keys(%data) ) {
for key, val in data.iteritems():
    print "%12s %15ld %15ld" % ( key, val['written'], val['read'] )
#     printf( "%12s %15ld %15ld\n", $key, $data{$key}{'written'}, $data{$key}{'read'} );
# }
