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

#ifndef CONFIG_FILE
#define CONFIG_FILE "/etc/opt/nersc/slurmd_log_rotate_mq.conf"
#endif

#ifndef MAX_SERVERS
#define MAX_SERVERS 256
#endif

char *trim(char *string);

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
    char *routingKey;
    size_t max_transmit_size;
    int useSSL;
};

void printConfig(struct config *config, FILE *out) {
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
    fprintf(out, "exchangeType: %s\n", config->exchange_type);
    fprintf(out, "queue: %s\n", config->queue);
    fprintf(out, "routingKey: %s\n", config->routingKey);
    fprintf(out, "max_transmit_size: %lu\n", config->max_transmit_size);
    fprintf(out, "useSSL: %d\n", config->useSSL);
}

struct config *readConfig() {
    FILE *fp = fopen(CONFIG_FILE, "r");
    if (fp == NULL) return NULL;

    struct config *config = (struct config *) malloc(sizeof(struct config));
    if ( !config ) return NULL;

    memset(config, 0, sizeof(struct config));
    
    char *linePtr = NULL;
    size_t linePtr_sz = 0;

    for ( ; !feof(fp) && !ferror(fp); ) {
        size_t nread = getline(&linePtr, &linePtr_sz, fp);
        if (nread == 0 || feof(fp) || ferror(fp)) {
            break;
        }
        char *key = linePtr;
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
        } else if (strcmp(key, "exchangeType") == 0) {
            config->exchange_type = strdup(value);
        } else if (strcmp(key, "queue") == 0) {
            config->queue = strdup(value);
        } else if (strcmp(key, "routingKey") == 0) {
            config->routingKey = strdup(value);
        } else if (strcmp(key, "max_transmit_size") == 0) {
            config->max_transmit_size = strtoul(value, NULL, 10);
        } else if (strcmp(key, "useSSL") == 0) {
            config->useSSL= atoi(value);
        }
    }
    free(linePtr);
    return config;
}

char *trim(char *string) {
    if (string == NULL || strlen(string) == 0) {
        return string;
    }

    char *left = string;
    char *right = string + strlen(string) - 1;

    for ( ; left && *left && isspace(*left); left++ ) { }
    for ( ; right > left && right && *right && isspace(*right); right--) { }
    right++;
    *right = '\0';
    return left;
}


void die_on_amqp_error(amqp_rpc_reply_t x, char const *context)
{
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

int main(int argc, char **argv) {

    srand(time(NULL) ^ getpid());
    
    amqp_socket_t *socket;
    amqp_connection_state_t conn;
    amqp_rpc_reply_t reply;
    int status;

    struct config *config = readConfig();
    char *message = NULL;
    char *hostname;

    int connected;

    if ( !config ) {
        fprintf( stderr, "NULL config\n" );
        return 1;
    }
    else {
        fprintf( stderr, "Printing config\n" );
        printConfig( config, stdout );
    }

    for ( connected = 0, hostname = select_server(config); hostname != NULL ; hostname = select_server(config) ) {
        printf( "Attempting to connect to %s:%d\n", hostname, config->port );
        conn = amqp_new_connection();

        if ( config->useSSL )
            socket = amqp_ssl_socket_new(conn);
        else
            socket = amqp_tcp_socket_new(conn);

        if (socket == NULL) {
            fprintf(stderr, "Failed to create socket!\n");
            exit(1);
        }

        if ( config->useSSL ) {
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
    reply = amqp_get_rpc_reply(conn);
    die_on_amqp_error(reply, "channel open");

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
    reply = amqp_get_rpc_reply(conn);
    die_on_amqp_error(reply, "exchange declare");

    message = "hello world";
    if (message != NULL) {
        printf( "Sending message\n" );
        amqp_basic_properties_t props;

        /* TODO: figure out what these flags mean */
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG; 
        props.delivery_mode = 2; /* 1 or 2? */

        amqp_basic_publish(
                conn,                                   /* amqp_connection_state_t state */
                1,                                      /* amqp_channel_t channel */
                amqp_cstring_bytes(config->exchange),   /* amqp_bytes_t exchange */
                amqp_cstring_bytes(config->routingKey), /* amqp_bytes_t routing_key */
                0,                                      /* amqp_boolean_t mandatory */
                0,                                      /* amqp_boolean_t immediate */
                &props,                                 /* amqp_basic_properties_t properties */
                amqp_cstring_bytes(message)             /* amqp_bytes_t body */
        );
        reply = amqp_get_rpc_reply(conn);
        die_on_amqp_error(reply, "publish message");
    }

    reply = amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    die_on_amqp_error(reply, "channel close");

    reply = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    die_on_amqp_error(reply, "connection close");

    status = amqp_destroy_connection(conn);

    return 0;
}
