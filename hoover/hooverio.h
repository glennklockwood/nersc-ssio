#include <openssl/sha.h>
#include <zlib.h>

#ifndef HOOVER_BLK_SIZE
#define HOOVER_BLK_SIZE 128 * 1024
#endif

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1


/*
 * hoover_data_obj describes a file that has been loaded into memory through
 *   hoover_load_file()
 */
struct hoover_data_obj {
    char hash[SHA_DIGEST_LENGTH_HEX];
    char hash_orig[SHA_DIGEST_LENGTH_HEX];
    size_t size;
    size_t size_orig;
    void *data;
};

/*
 * function prototypes
 */

void hoover_free_hdo( struct hoover_data_obj *hdo );
size_t hoover_write_hdo( FILE *fp, struct hoover_data_obj *hdo, size_t block_size );
struct hoover_data_obj *hoover_load_file( FILE *fp, size_t block_size );
