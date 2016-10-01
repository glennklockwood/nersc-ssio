/*
 * wrapper to test basic file encoding features of hooverio
 */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include "hooverio.h"

int main( int argc, char **argv )
{
    FILE *fp_in, *fp_out;
    struct stat st;
    struct hoover_data_obj *hdo;

    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <input file> [output file]\n", argv[0] );
        return 1;
    }
    else if ( argc < 3 )
        fp_out = stdout;
    else
        fp_out = fopen( argv[2], "w" );
    fp_in = fopen( argv[1], "r" );

    if ( !fp_in ) {
        fprintf( stderr, "Could not open input file %s\n", argv[1] );
        return ENOENT;
    }
    else if ( !fp_out ) {
        fprintf( stderr, "Could not open output file %s\n", ( argc < 3 ? "stdout" : argv[2] ) );
        return ENOENT;
    }

    hdo = hoover_load_file( fp_in, HOOVER_BLK_SIZE );

    fclose(fp_in);

    if ( hdo != NULL ) {
        printf( "Loaded:        %ld bytes\n", hdo->size_orig );
        printf( "Original hash: %s\n",        hdo->hash_orig );
        printf( "Saving:        %ld bytes\n", hdo->size );
        printf( "Saved hash:    %s\n",        hdo->hash );
        hoover_write_hdo( fp_out, hdo, HOOVER_BLK_SIZE );
        hoover_free_hdo( hdo );
    }
    else {
        fprintf(stderr, "hoover_load_file failed (errno=%d)\n", errno );
        fclose(fp_out);
        return 1;
    }

    fclose(fp_out);

    return 0;
}
