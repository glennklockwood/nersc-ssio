#include <stdio.h>
#include <stdlib.h>
#include <lustre/lustreapi.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

void print_lum( struct lov_user_md *lum );

int main( int argc, char **argv ) {
    int ret;
    int fd;
    struct lov_user_md *lum;
    size_t lumsize = sizeof(struct lov_user_md) +
            LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);

    lum = calloc(1, lumsize);

    /* 
     * LL_IOC_LOV_GETSTRIPE takes only an open file handle as input.  It only
     *   returns stripe placement information if the lmm_stripe_count field of
     *   the inputted lov_user_md struct is set to a nonzero value that
     *   indicates how many lov_user_ost_data structures have been malloc'ed
     */
    if ( argc == 2 ) {
        char *input_filepath = argv[1];
        printf( "This is with ioctl LL_IOC_LOV_GETSTRIPE:\n" );
        lum->lmm_magic = LOV_USER_MAGIC;
        lum->lmm_stripe_count = LOV_MAX_STRIPE_COUNT;
        fd = open( input_filepath, O_RDWR );
        ret = ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum );
        close(fd);
        print_lum(lum);
    }
    /* 
     * IOC_MDC_GETFILESTRIPE takes an open directory handle and a file basename
     * string as input and returns both stripe geometry and stripe placement
     * information
     */
    else if ( argc == 3 ) {
        char *dirname  = argv[1];
        char *basename = argv[2];
        printf( "This is with IOC_MDC_GETFILESTRIPE:\n" );
        fd = open( dirname, O_RDONLY );
        strcpy( (char *)lum, basename );
        ret = ioctl( fd, IOC_MDC_GETFILESTRIPE, (void*)lum );
        print_lum(lum);
        close(fd);
    }

    return 0;
}

void print_lum( struct lov_user_md *lum ) {
    int i;
    printf( "Stripe size: %d\nStripe count: %d\nOST idx: %d\n",
        lum->lmm_stripe_size,
        lum->lmm_stripe_count,
        lum->lmm_objects[0].l_ost_idx );

    for ( i = 0; i < lum->lmm_stripe_count; i++ ) {

        printf( "  os_id: %ld oi_seq: %ld f_seq: %ld f_oid: %d f_ver: %d l_ost_gen: %d l_ost_idx: %d\n",
            lum->lmm_objects[i].l_ost_oi.oi.oi_id,
            lum->lmm_objects[i].l_ost_oi.oi.oi_seq,
            lum->lmm_objects[i].l_ost_oi.oi_fid.f_seq,
            lum->lmm_objects[i].l_ost_oi.oi_fid.f_oid,
            lum->lmm_objects[i].l_ost_oi.oi_fid.f_ver,
            lum->lmm_objects[i].l_ost_gen,
            lum->lmm_objects[i].l_ost_idx 
            );
    }
    return;
}

