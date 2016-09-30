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
    void *out_buf;
    size_t out_buf_len;
    int ret;

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

    /* get file size so we know how big to allocate our buffer */
    if ( fstat(fileno(fp_in), &st) != 0 ) {
        fclose(fp_in);
        fclose(fp_out);
        return 1;
    }
    else {
        /* worst-case scenario is that compression adds +10%; hopefully it will
         * be negative
         */
        out_buf_len = st.st_size * 1.1;
        out_buf = malloc(out_buf_len);
    }

    ret = process_file_by_block( fp_in, HOOVER_BLK_SIZE, out_buf, out_buf_len );

    out_buf_len = (size_t)ret;

    fclose(fp_in);

    if ( ret > 0 ) {
        printf( "Writing out %ld bytes\n", out_buf_len );
        write_buffer_by_block( fp_out, HOOVER_BLK_SIZE, out_buf, out_buf_len );
    }
    else {
        fprintf(stderr, "process_file_by_block returned %d\n", ret );
        fclose(fp_out);
        return 1;
    }

    fclose(fp_out);

    return 0;
}
