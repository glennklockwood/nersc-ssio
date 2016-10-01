/*  hooverio.c
 *
 *  Components of Hoover that are responsible for file I/O and processing data
 *  streams (compression, checksumming, etc)
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <assert.h> /* for debugging */

#include "hooverio.h"

/*******************************************************************************
 *  local prototypes and structs
 ******************************************************************************/
struct block_state_structs *init_block_states( void );
int *finalize_block_states( struct block_state_structs *bss );

/*
 * block_state_structs is just a container for the state structs that belong
 *   to all of the block processing algorithms (compression, encryption, etc)
 *   that may be used by hooverio
 */
struct block_state_structs {
    SHA_CTX sha_stream;
    SHA_CTX sha_stream_compressed;
    z_stream z_stream; 
    unsigned char sha_hash[SHA_DIGEST_LENGTH];
    unsigned char sha_hash_compressed[SHA_DIGEST_LENGTH];
    char sha_hash_hex[SHA_DIGEST_LENGTH_HEX];
    char sha_hash_compressed_hex[SHA_DIGEST_LENGTH_HEX];
};


/*******************************************************************************
 * internal functions
 ******************************************************************************/
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
struct hoover_data_obj *hoover_load_file( FILE *fp, size_t block_size ) {
    void *buf,
         *out_buf,
         *p_out;
    size_t out_buf_len,
           bytes_read,
           bytes_written,
           tot_bytes_read = 0,
           tot_bytes_written = 0;
    struct block_state_structs *bss;
    struct hoover_data_obj *hdo;
    struct stat st;
    int fail = 0;
    int flush;

    /* buf is filled from file, then processed (compress+hash) */
    if ( !(buf = malloc( block_size )) ) return NULL;

    /* get file size so we know how big to allocate our buffer */
    if ( fstat(fileno(fp), &st) != 0 ) {
        free( buf );
        return NULL;
    }

    /* worst-case, compression adds +10%; ideally it will reduce size */
    out_buf_len = st.st_size * 1.1;
    out_buf = malloc(out_buf_len);
    if ( !out_buf ) {
        free(buf);
        return NULL;
    }
    p_out = out_buf;

    /* initialize block-based algorithm state stuctures here */
    if ( !(bss = init_block_states()) ) {
        free(buf);
        free(out_buf);
        return NULL;
    }

    do { /* loop until no more input */
        bytes_read = fread(buf, 1, block_size, fp);

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

            /* update the pointer - there may be a cleaner way to do this */
            p_out = (bss->z_stream).next_out;
        } while ( (bss->z_stream).avail_out == 0 );
        if ( fail ) break;
    } while ( bytes_read != 0 ); /* loop until we run out of input */

    assert( bss->z_stream.avail_in == 0 );
    if ( bss->z_stream.avail_out != 0 )
    {
        deflate(&(bss->z_stream), flush);
        bytes_written = ( (char*)((bss->z_stream).next_out) - (char *)p_out );
        tot_bytes_written += bytes_written;
        SHA1_Update( &(bss->sha_stream_compressed), p_out, bytes_written );
    }

    assert( flush == Z_FINISH );

    /* finalize block-based algorithm state structures here */
    finalize_block_states( bss );

    /* create the hoover data object */
    hdo = malloc(sizeof(*hdo));
    if (!hdo) {
        free(buf);
        free(out_buf);
        return NULL;
    }
    strncpy( hdo->hash, bss->sha_hash_compressed_hex, SHA_DIGEST_LENGTH_HEX );
    strncpy( hdo->hash_orig, bss->sha_hash_hex, SHA_DIGEST_LENGTH_HEX );
    
    hdo->size = tot_bytes_written;
    hdo->data = realloc( out_buf, tot_bytes_written );
    hdo->size_orig = tot_bytes_read;

    /* clean up */
    free(buf);

    return hdo;
}


/* free memory associated with a hoover data object */
void hoover_free_hdo( struct hoover_data_obj *hdo ) {
    if ( hdo == NULL ) {
        fprintf( stderr, "hoover_data_obj: received NULL hdo\n" );
        return;
    }
    else {
        free( hdo->data );
        free( hdo );
    }
    return;
}


/*
 * Write a memory buffer to a file block by block
 */
size_t hoover_write_hdo( FILE *fp, struct hoover_data_obj *hdo, size_t block_size ) {
    void *p_out = hdo->data;
    size_t bytes_written,
           tot_bytes_written = 0,
           bytes_left = hdo->size;
    do {
        if ( bytes_left > block_size )
            bytes_written = fwrite( p_out, 1, block_size, fp );
        else
            bytes_written = fwrite( p_out, 1, bytes_left, fp );
        p_out = (char*)p_out + bytes_written;
        tot_bytes_written += bytes_written;
        bytes_left = hdo->size - tot_bytes_written;
    } while ( bytes_left != 0 );

    return tot_bytes_written;
}
