#include <stdio.h>
#include <openssl/sha.h>
#include <string.h>

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
    static unsigned char hex_hash[SHA_DIGEST_LENGTH * 2 + 1];
    int i;

    SHA1(blob, len, hash);

    /* note that hash does not terminate in \0 */
    for ( i = 0; i < SHA_DIGEST_LENGTH; i++ )
        sprintf( (char*)&(hex_hash[2*i]), "%02x", hash[i] );

    return hex_hash;
}

int main( int argc, char **argv )
{
    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <input string>\n", argv[0] );
        return 1;
    }

    printf( "Original string: [%s]\nSHA1 hash:       [%s]\n",
        argv[1],
        sha1_blob( (unsigned const char*)argv[1], strlen(argv[1]) ) );
    return 0;
}
