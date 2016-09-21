#include <stdio.h>
#include <openssl/sha.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>

#include <zlib.h>

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

#define BLK_SIZE 128 * 1024

/*
 * Calculate SHA1 hash of all data located behind a file pointer
 */
unsigned char *sha1_file( FILE *fp, size_t buf_size ) {
    SHA_CTX ctx;
    char *buf;
    size_t bytes_read;
    unsigned char hash[SHA_DIGEST_LENGTH];
    static unsigned char hex_hash[SHA_DIGEST_LENGTH_HEX];
    int i;

    if ( !(buf = malloc( buf_size )) ) return NULL;

    SHA1_Init(&ctx);
    do {
        bytes_read = fread(buf, 1, buf_size, fp);
        SHA1_Update( &ctx, buf, bytes_read );
    } while ( bytes_read != 0 );

    free( buf );

    SHA1_Final(hash, &ctx);

    /* note that hash does not terminate in \0 */
    for ( i = 0; i < SHA_DIGEST_LENGTH; i++ )
        sprintf( (char*)&(hex_hash[2*i]), "%02x", hash[i] );

    return hex_hash;
}

/*
 * Read a file block by block, and pass these blocks through block-based
 * algorithms (hashing, compression, etc)
 */
size_t process_file_by_block( FILE *fp, size_t block_size, void *out_buf ) {
    void *buf, *p_out;
    size_t bytes_read, tot_bytes_read = 0;
    buf = malloc( block_size );
    if ( !buf ) return -1;
    p_out = out_buf;

    /* initialize block-based algorithm state stuctures here */

    do {
        bytes_read = fread(buf, 1, block_size, fp);

        /* execute block-based algorithms here; update buf */

        memcpy( p_out, buf, bytes_read );
        p_out = (char*)p_out + bytes_read;
        tot_bytes_read += bytes_read;
    } while ( bytes_read != 0 );

    /* finalize block-based algorithm state structures here */

    return tot_bytes_read;
}

/*
 * Write a memory buffer to a file block by block
 */
size_t write_buffer_by_block( FILE *fp, size_t block_size, void *out_buf, size_t out_len ) {
    void *p_out = out_buf;
    size_t bytes_written,
           tot_bytes_written = 0,
           bytes_left = out_len;

    do {
        if ( bytes_left > block_size )
            bytes_written = fwrite( p_out, 1, block_size, fp );
        else
            bytes_written = fwrite( p_out, 1, bytes_left, fp );
        p_out = (char*)p_out + bytes_written;
        tot_bytes_written += bytes_written;
        bytes_left = out_len - tot_bytes_written;
    } while ( bytes_left != 0 );

    return tot_bytes_written;
}

/*
 *  compress a byte vector
 */
size_t compress_buffer(char *src, char *dest, size_t src_size, size_t dest_size, size_t buf_size)
{
    int ret, flush;
    z_stream strm;

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
    if (ret != Z_OK)
        return ret;

    strm.next_out = (unsigned char*)dest;
    strm.avail_out = dest_size;
    strm.next_in = (unsigned char*)src;
    strm.avail_in = src_size;

    while ( strm.total_in < src_size ) {
        if ( strm.total_in == 0 ) {
            /* out of buffer space for compression; abort */
            return -1;
        }

        deflate( &strm, Z_NO_FLUSH );
    }

    if ( (deflate(&strm, Z_FINISH)) != Z_STREAM_END ) {
        deflateEnd( &strm );
        return -1;
    }
    deflateEnd( &strm );
    return strm.total_out;
}


int main( int argc, char **argv )
{
    FILE *fp_in, *fp_out;
    struct stat st;
    void *out_buf;
    size_t out_buf_len;

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
        out_buf = malloc(st.st_size * 1.1);
    }

    out_buf_len = process_file_by_block( fp_in, BLK_SIZE, out_buf );

    fclose(fp_in);

    if ( out_buf_len > 0 ) {
        printf( "Writing out %ld bytes\n", out_buf_len );
        write_buffer_by_block( fp_out, BLK_SIZE, out_buf, out_buf_len );
    }
    else {
        fprintf(stderr, "process_file_by_block returned %ld bytes read\n", out_buf_len );
        return 1;
    }

    fclose(fp_out);

    return 0;
}
