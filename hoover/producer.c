/*******************************************************************************
 *  This code is derived from rabbitmq_slurmdlog.c in CSG/nersc-slurm.git,
 *  written by D. M. Jacobsen.
 *
 *  Glenn K. Lockwood, Lawrence Berkeley National Laboratory      September 2016
 ******************************************************************************/
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
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

#include "hooverio.h"

/* TODO: offer alternate options for systems not supporting SHA */
#include <openssl/sha.h>

#ifndef HOOVER_CONFIG_FILE
#define HOOVER_CONFIG_FILE "/etc/opt/nersc/slurmd_log_rotate_mq.conf"
#endif

#ifndef HOOVER_MAX_SERVERS
#define HOOVER_MAX_SERVERS 256
#endif

#define SHA_DIGEST_LENGTH_HEX SHA_DIGEST_LENGTH * 2 + 1

/*
 * Global structures
 */
struct config {
    char *servers[HOOVER_MAX_SERVERS];
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

/* each hoover_tube just aggregates a connection, a socket, a channel, and an
   exchange into a single object for simplicity.  For simple message passing,
   we only need to define one of each to send messages. */
struct hoover_tube {
    amqp_socket_t *socket;
    amqp_channel_t channel; /* I have no idea why channel passed around by value by rabbitmq-c */
    amqp_connection_state_t connection;
};

/*
 *  Function prototypes
 */
void send_message(amqp_connection_state_t conn, amqp_channel_t channel,
                  amqp_bytes_t *body, char *exchange, char *routing_key,
                  struct hoover_header *header);
void save_config(struct config *config, FILE *out);
int parse_amqp_response(amqp_rpc_reply_t x, char const *context, int die);
char *trim(char *string);
char *select_server(struct config *config);
struct hoover_tube *create_hoover_tube(struct config *config);
void destroy_hoover_tube( struct hoover_tube *tube );
struct config *read_config();
struct hoover_header *build_header( char *filename, struct hoover_data_obj *hdo );

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
    FILE *fp = fopen(HOOVER_CONFIG_FILE, "r");
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

                if ( server_cnt < HOOVER_MAX_SERVERS ) {
                    config->servers[server_cnt] = strdup(t_value);
                    printf( "Found server %s\n", config->servers[server_cnt] );
                    server_cnt++;
                }
                else {
                    fprintf( stderr, "too many servers in config file; truncating at %d\n", HOOVER_MAX_SERVERS );
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
int parse_amqp_response(amqp_rpc_reply_t x, char const *context, int die) {
    amqp_connection_close_t *conn_close_reply;
    amqp_channel_close_t *chan_close_reply;
    switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return 0;

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
    if ( die )
        exit(1);
    else
        return 1;
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


void destroy_amqp_table( amqp_table_t *table ) {
    int i;
    for ( i = 0; i < table->num_entries; i++ ) {
        free(&(table->entries[i]));
    }
    free(table);
    return;
}

/* create_amqp_header_table - convert a hoover_header struct into an AMQP table
 * to be attached to a message
 */
amqp_table_t *create_amqp_header_table( struct hoover_header *header ) {
    amqp_table_t *table;
    amqp_table_entry_t *entries;

    if ( !(table = malloc(sizeof(*table))) )
        return NULL;
    if ( !(entries = malloc(3 * sizeof(*entries))) ) {
        free(table);
        return NULL;
    }

    table->num_entries = 3;

    /* Set headers */
    entries[0].key = amqp_cstring_bytes("filename");
    entries[0].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[0].value.value.bytes = amqp_cstring_bytes(header->filename);

    entries[1].key = amqp_cstring_bytes("size");
    entries[1].value.kind = AMQP_FIELD_KIND_I64;
    entries[1].value.value.i64 = header->size;

    entries[2].key = amqp_cstring_bytes("checksum");
    entries[2].value.kind = AMQP_FIELD_KIND_UTF8;
    entries[2].value.value.bytes = amqp_cstring_bytes((char*)header->hash);

    table->entries = entries;

    return table;
}

/* convert a bunch of runtime structures into an AMQP message and send it  */
void send_message( amqp_connection_state_t conn, amqp_channel_t channel,
                   amqp_bytes_t *body, char *exchange, char *routing_key,
                   struct hoover_header *header ) {
    amqp_rpc_reply_t reply;
    amqp_basic_properties_t props;
    amqp_table_t *table;

    memset( &props, 0, sizeof(props) );

    /* create the amqp_table that contains the header metadata */
    table = create_amqp_header_table( header );

    /* TODO: figure out what these flags mean */
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_HEADERS_FLAG;
    props.delivery_mode = 2; /* 1 or 2? */
    props.headers = *table;

    /* Send the actual AMQP message */
    printf( "Sending message\n" );
    amqp_basic_publish(
        conn,                               /* amqp_connection_state_t state */
        1,                                  /* amqp_channel_t channel */
        amqp_cstring_bytes(exchange),       /* amqp_bytes_t exchange */
        amqp_cstring_bytes(routing_key),    /* amqp_bytes_t routing_key */
        0,                                  /* amqp_boolean_t mandatory */
        0,                                  /* amqp_boolean_t immediate */
        &props,                             /* amqp_basic_properties_t properties */
        *body                               /* amqp_bytes_t body */
    );

    /* no longer need the header table */
    free(table->entries);
    free(table);

    reply = amqp_get_rpc_reply(conn);
    parse_amqp_response(reply, "publish message", true);

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

/* generate the hoover_header struct from a file */
struct hoover_header *build_header( char *filename, struct hoover_data_obj *hdo ) {
    struct hoover_header *header;
    struct stat st;

    header = malloc(sizeof(struct hoover_header));
    if ( !header )
        return NULL;

    strncpy( header->filename, filename, MAX_PATH );

    header->size = hdo->size;

    strncpy( (char*)header->hash, (const char*)hdo->hash, SHA_DIGEST_LENGTH_HEX );

    printf( "file=[%s],size=[%ld],sha=[%s]\n",
        header->filename,
        header->size,
        header->hash );

    return header;
}

struct hoover_tube *create_hoover_tube(struct config *config) {
    int connected, status;
    char *hostname;
    struct hoover_tube *tube;
    amqp_rpc_reply_t reply;

    if (!(tube = malloc(sizeof(*tube))))
        return NULL;

    /* establish socket */
    for (connected = 0, hostname = select_server(config);
                        hostname != NULL;
                        hostname = select_server(config) ) {
        printf( "Attempting to connect to %s:%d\n", hostname, config->port );
        tube->connection = amqp_new_connection();

        if ( config->use_ssl )
            tube->socket = amqp_ssl_socket_new(tube->connection);
        else
            tube->socket = amqp_tcp_socket_new(tube->connection);

        if (tube->socket == NULL) {
            fprintf(stderr, "Failed to create socket!\n");
            destroy_hoover_tube(tube);
            return NULL;
        }

        if ( config->use_ssl ) {
            amqp_ssl_socket_set_verify_peer(tube->socket, 0);
            amqp_ssl_socket_set_verify_hostname(tube->socket, 0);
        }

        status = amqp_socket_open(tube->socket, hostname, config->port);

        if (status != 0) {
            fprintf( stderr, "Failed to connect to %s:%d; moving on...\n", hostname, config->port );
        }
        else {
            connected = 1;
            break;
        }
    }

    /* make sure connection exists */
    if (!connected) {
        fprintf(stderr, "Failed to connect to any servers!\n");
        destroy_hoover_tube(tube);
        return NULL;
    }

    /* authenticate */
    reply = amqp_login(
        tube->connection,       /* amqp_connection_state_t state */
        config->vhost,          /* char const *vhost */
        0,                      /* int channel_max */
        131072,                 /* int frame_max */
        0,                      /* int heartbeat */
        AMQP_SASL_METHOD_PLAIN, /* amqp_sasl_method_enum sasl_method */
        config->username,
        config->password);

    if ( parse_amqp_response(reply, "login", false) ) {
        destroy_hoover_tube(tube);
        return NULL;
    }

    /* open channel */
    tube->channel = 1;
    amqp_channel_open(tube->connection, tube->channel);
    if ( parse_amqp_response(amqp_get_rpc_reply(tube->connection), "channel open", false) ) {
        destroy_hoover_tube(tube);
        return NULL;
    }

    amqp_exchange_declare(
        tube->connection,                         /* amqp_connection_state_t state */
        tube->channel,                            /* amqp_channel_t channel */
        amqp_cstring_bytes(config->exchange),     /* amqp_bytes_t exchange */
        amqp_cstring_bytes(config->exchange_type),/* amqp_bytes_t type */
        0,                                        /* amqp_boolean_t passive */
        0,                                        /* amqp_boolean_t durable */
        0,                                        /* amqp_boolean_t auto_delete */
        0,                                        /* amqp_boolean_t internal */
        amqp_empty_table                          /* amqp_table_t arguments */
    );
    if ( parse_amqp_response(amqp_get_rpc_reply(tube->connection), "exchange declare", false) ) {
        destroy_hoover_tube(tube);
        return NULL;
    }

    return tube;
}

void destroy_hoover_tube( struct hoover_tube *tube ) {
    free(tube);
    return;
}

int main(int argc, char **argv) {
    amqp_bytes_t message;
    int status;

    struct config *config;
    struct hoover_header *header;
    struct hoover_data_obj *hdo;
    struct hoover_tube *tube;
    FILE *fp;

    srand(time(NULL) ^ getpid());

    /*
     *  Initialize a bunch of nonsense
     */
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

    /*
     *  Set up the AMQP connection, socket, exchange, and channel
     */
    if ( (tube = create_hoover_tube(config)) == NULL ) {
        fprintf( stderr, "could not establish tube\n" );
        return 1;
    }

    /*
     *  Begin handling data here
     */
    /* Load file into hoover data object */
    fp = fopen( argv[1], "r" );
    if ( !fp ) {
        fprintf( stderr, "could not open file %s\n", argv[1] );
        return 1;
    }
    hdo = hoover_load_file( fp, HOOVER_BLK_SIZE );

    /* Build header */
    header = build_header( argv[1], hdo );
    if ( !header ) {
        fprintf( stderr, "got null header\n" );
        hoover_free_hdo( hdo );
        return 1;
    }

    /* Send the message */
    message.len = hdo->size;
    message.bytes = hdo->data;
    send_message( 
        tube->connection, 
        1,
        &message,
        config->exchange,
        config->routing_key,
        header );

    /* Tear down everything */
    free(header);
    hoover_free_hdo( hdo );

    parse_amqp_response(amqp_channel_close(tube->connection, 1, AMQP_REPLY_SUCCESS), "channel close", true);
    /* Closes both the socket and the connection */
    parse_amqp_response(amqp_connection_close(tube->connection, AMQP_REPLY_SUCCESS), "connection close", true);

    status = amqp_destroy_connection(tube->connection);

    destroy_hoover_tube(tube);

    return 0;
}
