#include <stdio.h>
#include <lustre/lustreapi.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <dirent.h>

int main( int argc, char **argv ) {
    int n_ost, n_mds;
    int ret;
    DIR *mount_dir;
    char *mount_path = argv[1];

    mount_dir = opendir( mount_path );
    if ( !mount_dir  ) return 1;

    /* n_ost and n_mds are used for both input and output to ioctl */
    n_ost = 0;
    n_mds = 1;

    ret = ioctl( dirfd(mount_dir), LL_IOC_GETOBDCOUNT, &n_ost );
    if ( ret < 0 ) return 1;

    ret = ioctl( dirfd(mount_dir), LL_IOC_GETOBDCOUNT, &n_mds );
    if ( ret < 0 ) return 1;

    closedir( mount_dir );

    printf( "OST count: %3d\nMDS count: %3d\n", n_ost, n_mds );

    return 0;
}
