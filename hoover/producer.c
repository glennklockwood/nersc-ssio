/*******************************************************************************
 *  This code is derived from rabbitmq_slurmdlog.c in CSG/nersc-slurm.git,
 *  written by D. M. Jacobsen.
 *
 *  Glenn K. Lockwood, Lawrence Berkeley National Laboratory      September 2016
 ******************************************************************************/
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <signal.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <amqp_ssl_socket.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#ifdef __APPLE__
#include <sys/syslimits.h>
#define MAX_PATH PATH_MAX
#endif

/* TODO: offer alternate options for systems not supporting SHA */
#include <openssl/sha.h>

#ifndef CONFIG_FILE
#define CONFIG_FILE "/etc/opt/nersc/slurmd_log_rotate_mq.conf"
#endif

#ifndef MAX_SERVERS
#define MAX_SERVERS 256
#endif

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

/*
 * Global structures
 */
struct config {
    char *servers[MAX_SERVERS];
    int max_hosts;
    int remaining_hosts;
    int port;
    char *vhost;
    char *username;
    char *password;
    char *exchange;
    char *exchange_type;
    char *queue;
    char *routing_key;
    size_t max_transmit_size;
    int use_ssl;
};

struct hoover_header {
    char filename[MAX_PATH];
    size_t size;
    unsigned char hash[SHA_DIGEST_LENGTH_HEX];
};

/*
 *  Function prototypes
 */
void send_message( amqp_connection_state_t conn, amqp_channel_t channel,
    char *body, char *exchange, char *routing_key, struct hoover_header *header );
void save_config(struct config *config, FILE *out);
void die_on_amqp_error(amqp_rpc_reply_t x, char const *context);
char *trim(char *string);
char *select_server(struct config *config);
struct config *read_config();
unsigned char *sha1_file( FILE *fp, size_t buf_size );
unsigned char *sha1_blob( unsigned const char *blob, size_t len );
struct hoover_header *build_header( char *filename );

/*
 * Command-line and RabbitMQ configuration parameters
 */
void save_config(struct config *config, FILE *out) {
    if (config == NULL || out == NULL) return;

    int i;
    fprintf(out, "servers: ");
    for ( i = 0 ; i < config->max_hosts; i++) {
        fprintf(out, "%s%s", (i == 0 ? "" : ", "), config->servers[i] );
    }
    fprintf(out, "\nport: %d\n", config->port);
    fprintf(out, "vhost: %s\n", config->vhost);
    fprintf(out, "username: %s\n", config->username);
    fprintf(out, "password: %s\n", config->password);
    fprintf(out, "exchange: %s\n", config->exchange);
    fprintf(out, "exchange_type: %s\n", config->exchange_type);
    fprintf(out, "queue: %s\n", config->queue);
    fprintf(out, "routing_key: %s\n", config->routing_key);
    fprintf(out, "max_transmit_size: %lu\n", config->max_transmit_size);
    fprintf(out, "use_ssl: %d\n", config->use_ssl);
}

struct config *read_config() {
    FILE *fp = fopen(CONFIG_FILE, "r");
    if (fp == NULL) return NULL;

    struct config *config = (struct config *) malloc(sizeof(struct config));
    if ( !config ) return NULL;

    memset(config, 0, sizeof(struct config));
    
    char *p = NULL;
    size_t ps = 0;

    while (!feof(fp) && !ferror(fp)) {
        size_t nread = getline(&p, &ps, fp);
        if (nread == 0 || feof(fp) || ferror(fp)) {
            break;
        }
        char *key = p;
        char *ptr = strchr(key, '=');
        if (ptr == NULL) continue;
        *ptr = 0;
        
        char *value = ptr + 1;
        key = trim(key);
        value = trim(value);

        if (strcmp(key, "servers") == 0) {
            char *save_ptr = NULL;
            char *t_value = NULL;
            char *search = value;
            size_t server_cnt = 0;

            while ((t_value = strtok_r(search, ",", &save_ptr)) != NULL) {
                search = NULL;
                t_value = trim(t_value);
                if (t_value == NULL) continue;

                if ( server_cnt < MAX_SERVERS ) {
                    config->servers[server_cnt] = strdup(t_value);
                    printf( "Found server %s\n", config->servers[server_cnt] );
                    server_cnt++;
                }
                else {
                    fprintf( stderr, "too many servers in config file; truncating at %d\n", MAX_SERVERS );
                    break;
                }
            }
            config->max_hosts = server_cnt;
            config->remaining_hosts = server_cnt;
        } else if (strcmp(key, "port") == 0) {
            config->port = atoi(value);
        } else if (strcmp(key, "vhost") == 0) {
            config->vhost = strdup(value);
        } else if (strcmp(key, "username") == 0) {
            config->username = strdup(value);
        } else if (strcmp(key, "password") == 0) {
            config->password = strdup(value);
        } else if (strcmp(key, "exchange") == 0) {
            config->exchange = strdup(value);
        } else if (strcmp(key, "exchangeType") == 0 || strcmp(key, "exchange_type") == 0) {
            config->exchange_type = strdup(value);
        } else if (strcmp(key, "queue") == 0) {
            config->queue = strdup(value);
        } else if (strcmp(key, "routingKey") == 0 || strcmp(key, "routing_key") == 0) {
            config->routing_key = strdup(value);
        } else if (strcmp(key, "maxTransmitSize") == 0 || strcmp(key, "max_transmit_size") == 0) {
            config->max_transmit_size = strtoul(value, NULL, 10);
        } else if (strcmp(key, "use_ssl") == 0) {
            config->use_ssl = atoi(value);
        }
    }
    free(p);
    return config;
}

/*
 * Strip leading/trailing whitespace from a string
 */
char *trim(char *string) {
    if (string == NULL || strlen(string) == 0)
        return string;

    char *left = string;
    char *right = string + strlen(string) - 1;

    while (left && *left && isspace(*left))
        left++;
    while (right > left && right && *right && isspace(*right))
        right--;
    right++;
    *right = '\0';
    return left;
}


/*
 * Check return of a rabbitmq-c call and throw an error + clean up if it is a
 * failure
 */
void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
    amqp_connection_close_t *conn_close_reply;
    amqp_channel_close_t *chan_close_reply;
    switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return;

    case AMQP_RESPONSE_NONE:
        fprintf(stderr, "%s: missing RPC reply type!\n", context);
        break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
        break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (x.reply.id) {
            case AMQP_CONNECTION_CLOSE_METHOD:
                conn_close_reply = (amqp_connection_close_t *) x.reply.decoded;
                fprintf(stderr, "%s: server connection error %d, message: %.*s\n",
                    context,
                    conn_close_reply->reply_code,
                    (int)conn_close_reply->reply_text.len, (char *)conn_close_reply->reply_text.bytes);
                break;
            case AMQP_CHANNEL_CLOSE_METHOD:
                chan_close_reply = (amqp_channel_close_t *) x.reply.decoded;
                fprintf(stderr, "%s: server channel error %d, message: %.*s\n",
                    context,
                    chan_close_reply->reply_code,
                    (int)chan_close_reply->reply_text.len, (char *)chan_close_reply->reply_text.bytes);
                break;
            default:
                fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
                break;
        }
        break;
    }
    exit(1);
}

/*
 *  randomly select a server from the list of servers, then pop it off the list
 */
char *select_server(struct config *config) {
    if (config->remaining_hosts == 0 || config->max_hosts == 0) return NULL;

    size_t idx;
    char *server;

    idx = rand() % config->max_hosts;
    server = config->servers[idx];

    /* swap last element with selected element */
    config->servers[idx] = config->servers[config->remaining_hosts - 1];

    /* shorten the candidate list so we don't try the same server twice */
    config->remaining_hosts--;
    return server;
}


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




int main(int argc, char **argv) {

    srand(time(NULL) ^ getpid());
    
    amqp_socket_t *socket;
    amqp_connection_state_t conn;
    amqp_rpc_reply_t reply;
    int status;

    struct config *config;
    struct hoover_header *header;
    char *message = NULL;
    char *hostname;

    int connected;

    if ( !(config = read_config()) ) {
        fprintf( stderr, "NULL config\n" );
        return 1;
    }
    else {
        save_config( config, stdout );
    }

    if ( argc < 2 ) {
        fprintf( stderr, "Syntax: %s <file name>\n", argv[0] );
        return 1;
    }

    for ( connected = 0, hostname = select_server(config); hostname != NULL ; hostname = select_server(config) ) {
        printf( "Attempting to connect to %s:%d\n", hostname, config->port );
        conn = amqp_new_connection();

        if ( config->use_ssl )
            socket = amqp_ssl_socket_new(conn);
        else
            socket = amqp_tcp_socket_new(conn);

        if (socket == NULL) {
            fprintf(stderr, "Failed to create socket!\n");
            exit(1);
        }

        if ( config->use_ssl ) {
            amqp_ssl_socket_set_verify_peer(socket, 0);
            amqp_ssl_socket_set_verify_hostname(socket, 0);
        }

        status = amqp_socket_open(socket, hostname, config->port);

        if (status != 0) {
            fprintf( stderr, "Failed to connect to %s:%d; moving on...\n", hostname, config->port );
        }
        else {
            connected = 1;
            break;
        }
    }

    if (!connected) {
        fprintf(stderr, "Failed to connect to any servers!\n");
        exit(1);
    }

    reply = amqp_login(
            conn,                            /* amqp_connection_state_t state */
            config->vhost,                               /* char const *vhost */
            0,                                             /* int channel_max */
            131072,                                          /* int frame_max */
            0,                                               /* int heartbeat */
            AMQP_SASL_METHOD_PLAIN,      /* amqp_sasl_method_enum sasl_method */
            config->username,
            config->password);
    die_on_amqp_error(reply, "login");

    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "channel open");

    amqp_exchange_declare(
            conn,                            /* amqp_connection_state_t state */
            1,                                      /* amqp_channel_t channel */
            amqp_cstring_bytes(config->exchange),    /* amqp_bytes_t exchange */
            amqp_cstring_bytes(config->exchange_type),    /* amqp_bytes_t type */
            0,                                      /* amqp_boolean_t passive */
            0,                                      /* amqp_boolean_t durable */
            0,                                  /* amqp_boolean_t auto_delete */
            0,                                     /* amqp_boolean_t internal */
            amqp_empty_table                        /* amqp_table_t arguments */
    );
    die_on_amqp_error(amqp_get_rpc_reply(conn), "exchange declare");

    /* build headers here */
    message = "hello world";
    header = build_header( argv[1] );
    if ( !header ) {
        fprintf( stderr, "got null header\n" );
        return 1;
    }

    /* send the message */
    send_message( 
        conn, 
        1,
        message,
        config->exchange,
        config->routing_key,
        header );

    free(header);

    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "channel close");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "connection close");

    status = amqp_destroy_connection(conn);

    return 0;
}

/*
 * input: hoover_header, amqp_basic_properties_t
 * output: none
 * side-effects: transfer the header data from hoover_header to
 *               amqp_basic_properties_t
 */
int set_hoover_header( struct hoover_header *header, amqp_basic_properties_t *props ) {


    return 0;
}

void destroy_amqp_table( amqp_table_t *table ) {
    int i;
    for ( i = 0; i < table->num_entries; i++ ) {
        free(&(table->entries[i]));
    }
    free(table);
    return;
}

void send_message( amqp_connection_state_t conn, amqp_channel_t channel,
                   char *body, char *exchange, char *routing_key,
                   struct hoover_header *header ) {
    amqp_rpc_reply_t reply;
    amqp_basic_properties_t props;
    amqp_table_t table;
    amqp_table_entry_t entries[3];

    memset( &props, 0, sizeof(props) );

    /* TODO: figure out what these flags mean */
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_HEADERS_FLAG;
    props.delivery_mode = 2; /* 1 or 2? */

    /*
     * Set headers
     */
    entries[0].key = amqp_cstring_bytes("filename");
    entries[0].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[0].value.value.bytes = amqp_cstring_bytes(header->filename);

    entries[1].key = amqp_cstring_bytes("size");
    entries[1].value.kind = AMQP_FIELD_KIND_I64;
    entries[1].value.value.i64 = header->size;

    entries[2].key = amqp_cstring_bytes("checksum");
    entries[2].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[2].value.value.bytes = amqp_cstring_bytes((char*)header->hash);

    table.num_entries = 3;
    table.entries = entries;
    props.headers = table;

    printf( "Sending message\n" );
    amqp_basic_publish(
        conn,                                   /* amqp_connection_state_t state */
        1,                                      /* amqp_channel_t channel */
        amqp_cstring_bytes(exchange),           /* amqp_bytes_t exchange */
        amqp_cstring_bytes(routing_key),        /* amqp_bytes_t routing_key */
        0,                                      /* amqp_boolean_t mandatory */
        0,                                      /* amqp_boolean_t immediate */
        &props,                                 /* amqp_basic_properties_t properties */
        amqp_cstring_bytes(body)                /* amqp_bytes_t body */
    );
    reply = amqp_get_rpc_reply(conn);
    die_on_amqp_error(reply, "publish message");
    return;
}

/*
 * input: list of file name strings
 * output: list of hoover headers...or just the json blob?  python version
 *         returns a dict which gets serialized later
 *
struct hoover_header **build_manifest( char **filenames ) {
}
 */

struct hoover_header *build_header( char *filename ) {
    struct hoover_header *header;
    unsigned char *hex_hash;
    struct stat st;
    FILE *fp;

    header = malloc(sizeof(struct hoover_header));
    if ( !header )
        return NULL;

    if ( !(fp = fopen( filename, "r" )) ) {
        free(header);
        return NULL;
    }

    if ( fstat(fileno(fp), &st) != 0 ) {
        free(header);
        fclose(fp);
        return NULL;
    }

    fclose(fp);

    hex_hash = sha1_file( fp, 32*1024 /* 32 kib is arbitrary */ );

    printf( "hex_hash is [%s]\n", hex_hash );

    strncpy( header->filename, filename, MAX_PATH );

    header->size = st.st_size;

    strncpy( (char*)header->hash, (const char*)hex_hash, SHA_DIGEST_LENGTH_HEX );

    printf("file=[%s],size=[%ld],sha=[%s]\n", header->filename, header->size, header->hash );

    return header;
}