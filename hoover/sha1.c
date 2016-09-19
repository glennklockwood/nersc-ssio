#include <stdio.h>
#include <openssl/sha.h>

int main()
{  
    unsigned const char str[] = "hello world";
    unsigned char hash[SHA_DIGEST_LENGTH]; // == 20
    int i;

    SHA1(str, sizeof(str) - 1, hash);

    printf( "Original string: [%s]\nSHA1 hash: [", str );
    for (i = 0;i < SHA_DIGEST_LENGTH; i++) {
        printf( "%02x", hash[i] );
    }
    printf("]\n");

    return 0;
}
