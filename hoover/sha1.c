#include <stdio.h>
#include <openssl/sha.h>
#include <string.h>
#include <stdlib.h>

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

/* sha1_blob: Turn a byte blob into a hexified sha1 sum.  Not thread safe.
 *
 * input: blob - a byte array
 *        len - length of that byte array
 * output: SHA1 hash with each byte expressed as a hex value
 *
 * Note that hashing strings should NOT include the terminal NULL
 * byte since that is a C construct.  Also be careful when using
 * strlen() vs. sizeof(); sizeof() includes padding bytes if blob
 * is not aligned to 8 bytes.
 */
unsigned char *sha1_blob( unsigned const char *blob, size_t len ) {
    unsigned char hash[SHA_DIGEST_LENGTH];
    static unsigned char hex_hash[SHA_DIGEST_LENGTH_HEX];
    int i;

    SHA1(blob, len, hash);

    /* note that hash does not terminate in \0 */
    for ( i = 0; i < SHA_DIGEST_LENGTH; i++ )
        sprintf( (char*)&(hex_hash[2*i]), "%02x", hash[i] );

    return hex_hash;
}

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

    buf = malloc( buf_size );
    if ( !buf ) return NULL;

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

int main( int argc, char **argv )
{
    FILE *fp;

    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <input string>\n", argv[0] );
        return 1;
    }

    /* try to open a file called argv[1].  if it's not openable, assume argv[1]
     * is a string
     */
    fp = fopen( argv[1], "r" );
    if ( !fp ) {
        printf( "Original string: [%s]\nSHA1 hash:       [%s]\n",
            argv[1],
            sha1_blob( (unsigned const char*)argv[1], strlen(argv[1]) ) );
    }
    else {
        printf( "Original file: [%s]\nSHA1 hash:     [%s]\n",
            argv[1],
            sha1_file( fp, 1024 ) );
        fclose(fp);
    }
    return 0;
}
