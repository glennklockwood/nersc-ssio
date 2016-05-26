#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <lustre/lustre_user.h>
#include <fcntl.h>

#ifndef MEM_LIMIT
    #define MEM_LIMIT 1263
#endif

typedef int darshan_record_id;
typedef int UT_hash_handle;

/*******************************************************************************
 *
 *  Everything below here is lifted from darshan-lustre-log-format.h 
 *
 ******************************************************************************/

#define LUSTRE_COUNTERS \
    /* number of OSTs for file system */\
    X(LUSTRE_OSTS) \
    /* number of MDTs for file system */\
    X(LUSTRE_MDTS) \
    /* index of first OST for file */\
    X(LUSTRE_STRIPE_OFFSET) \
    /* bytes per stripe for file */\
    X(LUSTRE_STRIPE_SIZE) \
    /* number of stripes (OSTs) for file */\
    X(LUSTRE_STRIPE_WIDTH) \
    /* end of counters */\
    X(LUSTRE_NUM_INDICES)

#define X(a) a,
/* integer statistics for Lustre file records */
enum darshan_lustre_indices
{
    LUSTRE_COUNTERS
};
#undef X


struct darshan_lustre_record
{
    darshan_record_id rec_id;
    int64_t rank;
    int64_t counters[LUSTRE_NUM_INDICES];
    int64_t ost_ids[1];
};

struct lustre_record_runtime
{
    struct darshan_lustre_record *record;
    UT_hash_handle hlink;
};

struct lustre_runtime
{
    int    record_count;   /* number of records defined */
    size_t record_buffer_size; /* size of the allocated buffer pointed to by record_buffer */
    void   *next_free_record;  /* pointer to end of record_buffer */
    void   *record_buffer;     /* buffer in which records are created */
    struct lustre_record_runtime *record_runtime_array;
    struct lustre_record_runtime *record_hash;
};

static struct lustre_runtime *lustre_runtime;

void lustre_runtime_initialize()
{
    int mem_limit = MEM_LIMIT;
    int max_records;

    lustre_runtime = malloc(sizeof(*lustre_runtime));
    if(!lustre_runtime)
        return;
    memset(lustre_runtime, 0, sizeof(*lustre_runtime));

    /* allocate the full size of the memory limit we are given */
    lustre_runtime->record_buffer= malloc(mem_limit);
    if(!lustre_runtime->record_buffer)
    {
        lustre_runtime->record_buffer_size = 0;
        return;
    }
    lustre_runtime->record_buffer_size = mem_limit;
    lustre_runtime->next_free_record = lustre_runtime->record_buffer;
    memset(lustre_runtime->record_buffer, 0, lustre_runtime->record_buffer_size);

    /* Allocate array of Lustre runtime data.  We calculate the maximum possible
     * number of records that will fit into mem_limit by assuming that each
     * record has the minimum possible OST count, then allocate that many 
     * runtime records.  record_buffer will always run out of memory before
     * we overflow record_runtime_array.
     */
    max_records = mem_limit / sizeof(struct darshan_lustre_record);
    printf( "There can be a maximum of %ld records/runtime records\n", max_records );
    lustre_runtime->record_runtime_array =
        malloc( max_records * sizeof(struct lustre_record_runtime));
    if(!lustre_runtime->record_runtime_array)
    {
        /* back out of initializing this module's records */
        lustre_runtime->record_buffer_size = 0;
        free( lustre_runtime->record_buffer );
        return;
    }
    memset(lustre_runtime->record_runtime_array, 0,
        max_records * sizeof(struct lustre_record_runtime));

    return;
}

#define LUSTRE_RECORD_SIZE( osts ) ( sizeof(struct darshan_lustre_record) + sizeof(int64_t) * (osts - 1) )

void darshan_instrument_lustre_file(const char* filepath, int fd)
{
    struct lustre_record_runtime *rec_rt;
    struct darshan_lustre_record *rec;
    darshan_record_id rec_id;
    int limit_flag;
    int i;
    struct lov_user_md *lum;
    size_t lumsize = sizeof(struct lov_user_md) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data);
    size_t rec_size;

    /* if we can't issue ioctl, we have no counter data at all */
    if ( (lum = calloc(1, lumsize)) == NULL )
        return;

    /* find out the OST count of this file so we can allocate memory */
    lum->lmm_magic = LOV_USER_MAGIC;
    lum->lmm_stripe_count = LOV_MAX_STRIPE_COUNT;

    /* -1 means ioctl failed, likely because file isn't on Lustre */
    if ( ioctl( fd, LL_IOC_LOV_GETSTRIPE, (void *)lum ) == -1 )
    {
        free(lum);
        return;
    }

    rec_size = LUSTRE_RECORD_SIZE( lum->lmm_stripe_count );

    printf( "Found %ld stripes\n", lum->lmm_stripe_count );
    printf( "Memory buffer is %ld bytes\n", lustre_runtime->record_buffer_size );
    printf( "Base record size is %ld bytes\n", sizeof(struct darshan_lustre_record));
    printf( "Record size will be %ld bytes\n", rec_size );

    {
        /* broken out for clarity */
        void *end_of_new_record = (char*)lustre_runtime->next_free_record + rec_size;
        void *end_of_rec_buffer = (char*)lustre_runtime->record_buffer + lustre_runtime->record_buffer_size;
        limit_flag = ( end_of_new_record > end_of_rec_buffer );
    }
    /* check if adding this record would run us out of memory */
    if ( limit_flag )
    {
        printf( "This record would start at %ld and end at %ld, but our max addressable is %ld\n",
            (char*)lustre_runtime->next_free_record - (char*)lustre_runtime->record_buffer,
            (char*)lustre_runtime->next_free_record + rec_size - (char*)lustre_runtime->record_buffer,
            (char*)lustre_runtime->record_buffer + lustre_runtime->record_buffer_size - (char*)lustre_runtime->record_buffer);

        printf( "Out of memory!\n" );
        free(lum);
        return;
    }
    
    /* search the hash table for this file record, and initialize if not found */
    /* HASH_FIND( ... ) */

    /* add new file record to list */
    rec_rt = &(lustre_runtime->record_runtime_array[lustre_runtime->record_count]);
    rec_rt->record = lustre_runtime->next_free_record;
    lustre_runtime->next_free_record = (char*)(lustre_runtime->next_free_record) + rec_size;
    rec = rec_rt->record;
    rec->rec_id = lustre_runtime->record_count; /* XXX */
    rec->rank = 0; /* XXX */

    rec->counters[LUSTRE_STRIPE_SIZE] = lum->lmm_stripe_size;
    rec->counters[LUSTRE_STRIPE_WIDTH] = lum->lmm_stripe_count;
    rec->counters[LUSTRE_STRIPE_OFFSET] = lum->lmm_stripe_offset;
    for ( i = 0; i < lum->lmm_stripe_count; i++ )
        rec->ost_ids[i] = lum->lmm_objects[i].l_ost_idx;
    free(lum);

    printf( "This record starts at %ld\n", (char*)rec - (char*)lustre_runtime->record_buffer );
    printf( "This record's last element starts at %ld according to what we just wrote\n", 
        (char*)(&(rec->ost_ids[rec->counters[LUSTRE_STRIPE_WIDTH] - 1])) - (char*)(lustre_runtime->record_buffer) );
    printf( "This record's upper bound is %ld\n",
        (char*)(rec) + rec_size - (char*)(lustre_runtime->record_buffer) );

    lustre_runtime->record_count++;

    return;
}

int main( int argc, char **argv )
{
    int i, j;
    int fd;
    char *fname;
    lustre_runtime_initialize();

    for ( i = 1; i < argc; i++ )
    {
        fname = argv[i];
        printf( "\nProcessing %s\n", fname );
        fd = open( fname, O_RDONLY );
        darshan_instrument_lustre_file( fname, fd );
        close(fd);
    }

    /* print what we just loaded */
    for ( i = 0; i < lustre_runtime->record_count; i++ )
    {
        struct lustre_record_runtime *lrr;
        lrr = &(lustre_runtime->record_runtime_array[i]);
        printf( "File %2d\n", i );
        for ( j = 0; j < LUSTRE_NUM_INDICES; j++ )
        {
            printf( "  Counter %2d: %ld\n", j, lrr->record->counters[j] );
        }
        for ( j = 0; j < lrr->record->counters[LUSTRE_STRIPE_WIDTH]; j++ )
        {
            printf( "  Stripe %2d: %ld\n", j, lrr->record->ost_ids[j] );
        }
    }
    free(lustre_runtime->record_runtime_array);
    free(lustre_runtime->record_buffer);
    free(lustre_runtime);
    return 0;
}
