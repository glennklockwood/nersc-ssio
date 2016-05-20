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
    char *input_file= argv[1];
    struct lov_user_md *lum;
    size_t lumsize = sizeof(struct lov_user_md) +
            LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);


    lum = calloc(1, lumsize);

    printf( "This is with ioctl LL_IOC_LOV_GETSTRIPE:\n" );
    lum->lmm_magic = LOV_USER_MAGIC;
    fd = open( input_file, O_RDWR );
    ret = ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum );
    close(fd);
    print_lum(lum);

    memset(lum, 0, lumsize);

    printf( "\nThis is with llapi_file_get_stripe (which uses IOC_MDC_GETFILESTRIPE):\n" );
    llapi_file_get_stripe( input_file, lum );
    print_lum(lum);

    /* argv[1] is the directory containing the file named in argv[2] */
    if ( argc == 3 ) {
        char *basename = argv[2];
        char *dirname = argv[1];
        memset(lum, 0, lumsize);
        printf( "\nThis is with IOC_MDC_GETFILESTRIPE directly:\n" );
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

