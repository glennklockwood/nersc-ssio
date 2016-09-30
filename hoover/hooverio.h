#include <openssl/sha.h>
#include <zlib.h>

#ifndef HOOVER_BLK_SIZE
#define HOOVER_BLK_SIZE 128 * 1024
#endif

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

/*
 * structs
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

/*
 * function prototypes
 */
struct block_state_structs *init_block_states( void );
int *finalize_block_states( struct block_state_structs *bss );
size_t process_file_by_block( FILE *fp, size_t block_size, void *out_buf, size_t out_buf_len );
size_t write_buffer_by_block( FILE *fp, size_t block_size, void *out_buf, size_t out_len );
