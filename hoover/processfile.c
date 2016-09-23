/*  hoover-io.c
 *
 *  Components of Hoover that are responsible for file I/O and processing data
 *  streams (compression, checksumming, etc)
 */
#include <stdio.h>
#include <openssl/sha.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <assert.h> /* for debugging */

#include <zlib.h>

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

#define BLK_SIZE 128 * 1024

struct block_state_structs {
    SHA_CTX sha_stream;
    SHA_CTX sha_stream_compressed;
    z_stream z_stream; 
    unsigned char sha_hash[SHA_DIGEST_LENGTH];
    unsigned char sha_hash_compressed[SHA_DIGEST_LENGTH];
    char sha_hash_hex[SHA_DIGEST_LENGTH_HEX];
    char sha_hash_compressed_hex[SHA_DIGEST_LENGTH_HEX];
};


struct block_state_structs *init_block_states( void ) {
    struct block_state_structs *bss;

    bss = malloc(sizeof(*bss));
    if ( !bss ) return NULL;

    /* init SHA calculator */
    SHA1_Init( &(bss->sha_stream) );
    SHA1_Init( &(bss->sha_stream_compressed) );
    memset( bss->sha_hash, 0, SHA_DIGEST_LENGTH );
    memset( bss->sha_hash_compressed, 0, SHA_DIGEST_LENGTH );
    memset( bss->sha_hash_hex, 0, SHA_DIGEST_LENGTH_HEX );
    memset( bss->sha_hash_compressed_hex, 0, SHA_DIGEST_LENGTH_HEX );

    /* init zlib calculator */
    (bss->z_stream).zalloc = Z_NULL;
    (bss->z_stream).zfree = Z_NULL;
    (bss->z_stream).opaque = Z_NULL;

    if ( (deflateInit2(
            &(bss->z_stream),
            Z_DEFAULT_COMPRESSION,
            Z_DEFLATED,
            15 + 16, /* 15 is default for deflateInit, +32 enables gzip */
            8,
            Z_DEFAULT_STRATEGY)) != Z_OK ) {
        free(bss);
        return NULL;
    }

    return bss;
}

int *finalize_block_states( struct block_state_structs *bss ) {
    int i;

    deflateEnd( &(bss->z_stream) );
    SHA1_Final(bss->sha_hash, &(bss->sha_stream));
    SHA1_Final(bss->sha_hash_compressed, &(bss->sha_stream_compressed));

    for ( i = 0; i < SHA_DIGEST_LENGTH; i++ )
    {
        sprintf( (char*)&(bss->sha_hash_hex[2*i]), "%02x", bss->sha_hash[i] );
        sprintf( (char*)&(bss->sha_hash_compressed_hex[2*i]), "%02x", bss->sha_hash_compressed[i] );
    }
    /* final character is \0 from when bss was initialized */

    return 0;
}

/*
 * Read a file block by block, and pass these blocks through block-based
 * algorithms (hashing, compression, etc)
 */
size_t process_file_by_block( FILE *fp, size_t block_size, void *out_buf, size_t out_buf_len ) {
    void *buf,
         *p_out = out_buf;
    size_t bytes_read,
           bytes_written,
           tot_bytes_read = 0,
           tot_bytes_written = 0;
    struct block_state_structs *bss;
    int fail = 0;
    int flush;

    if ( !(buf = malloc( block_size )) ) return -1;

    /* initialize block-based algorithm state stuctures here */
    if ( !(bss = init_block_states()) ) {
        fprintf( stderr, "failed to init_block_states\n" );
        return -1;
    }

    do { /* loop until no more input */
        bytes_read = fread(buf, 1, block_size, fp);
        /* if ( ferror(fp) ) ... */
        fprintf( stderr, "input read (%ld)\n", bytes_read );

        if ( feof(fp) )
            flush = Z_FINISH;
        else
            flush = Z_NO_FLUSH;

        tot_bytes_read += bytes_read;
        /* update the SHA1 of the pre-compressed data */
        SHA1_Update( &(bss->sha_stream), buf, bytes_read );

        /* set start of compression block */
        (bss->z_stream).avail_in = bytes_read;
        (bss->z_stream).next_in = (unsigned char*)buf;

        do { /* loop until no more output */
            /* avail_out = how big is the output buffer */
            (bss->z_stream).avail_out = out_buf_len - tot_bytes_written;
            /* next_out = pointer to the output buffer */
            (bss->z_stream).next_out = (unsigned char*)out_buf + tot_bytes_written;

            /* deflate updates avail_in and next_in as it consumes input data.
               it may also update avail_out and next_out if it flushed any data,
               but this is not necessarily the case since zlib may internally
               buffer data */
            if ( (deflate(&(bss->z_stream), flush)) != Z_OK ) {
                fail = 1;
                break;
            }
            bytes_written = ( (char*)((bss->z_stream).next_out) - (char *)p_out );
            tot_bytes_written += bytes_written;

            /* update the SHA1 of the compressed */
            SHA1_Update( &(bss->sha_stream_compressed), p_out, bytes_written );

            fprintf( stderr, "tot_bytes_read = %ld; bss->z_stream.total_in = %ld\n", tot_bytes_read, (bss->z_stream).total_in );
            fprintf( stderr, "tot_bytes_written = %ld; bss->z_stream.total_out = %ld\n", tot_bytes_written, (bss->z_stream).total_out );

            /* update the pointer - there may be a cleaner way to do this */
            p_out = (bss->z_stream).next_out;
        } while ( (bss->z_stream).avail_out == 0 );
        if ( fail ) break;
    } while ( bytes_read != 0 ); /* loop until we run out of input */

    fprintf( stderr, "total bytes written = %ld\n", bss->z_stream.total_out );

    assert( bss->z_stream.avail_in == 0 );
    if ( bss->z_stream.avail_out != 0 )
    {
        fprintf( stderr, "doing one last flush\n" );
        deflate(&(bss->z_stream), flush);
        bytes_written = ( (char*)((bss->z_stream).next_out) - (char *)p_out );
        tot_bytes_written += bytes_written;
        SHA1_Update( &(bss->sha_stream_compressed), p_out, bytes_written );
    }

    fprintf( stderr, "total bytes written = %ld\n", bss->z_stream.total_out );
    assert( flush == Z_FINISH );

    /* finalize block-based algorithm state structures here */
    finalize_block_states( bss );

    fprintf( stderr, "SHA1 uncompressed = %s\n", bss->sha_hash_hex );
    fprintf( stderr, "SHA1 compressed = %s\n", bss->sha_hash_compressed_hex );

    return tot_bytes_written;
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

    ret = process_file_by_block( fp_in, BLK_SIZE, out_buf, out_buf_len );

    out_buf_len = (size_t)ret;

    fclose(fp_in);

    if ( ret > 0 ) {
        printf( "Writing out %ld bytes\n", out_buf_len );
        write_buffer_by_block( fp_out, BLK_SIZE, out_buf, out_buf_len );
    }
    else {
        fprintf(stderr, "process_file_by_block returned %d\n", ret );
        fclose(fp_out);
        return 1;
    }

    fclose(fp_out);

    return 0;
}
