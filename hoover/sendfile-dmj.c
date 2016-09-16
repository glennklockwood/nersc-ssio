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

struct config {
    char **servers;
    int port;
    char *vhost;
    char *username;
    char *password;
    char *exchange;
    char *exchangeType;
    char *queue;
    char *routingKey;
    size_t maxTransmitSize;

    int useSSL;
};

void printConfig(struct config *config, FILE *out) {
    if (config == NULL || out == NULL) return;

    char **servers = config->servers;
    fprintf(out, "servers: ");
    for ( ; servers && *servers; servers++) {
        fprintf(out, "%s%s", (servers == config->servers ? "" : "|"), *servers);
    }
    fprintf(out, "\nport: %d\n", config->port);
    fprintf(out, "vhost: %s\n", config->vhost);
    fprintf(out, "username: %s\n", config->username);
    fprintf(out, "password: %s\n", config->password);
    fprintf(out, "exchange: %s\n", config->exchange);
    fprintf(out, "exchangeType: %s\n", config->exchangeType);
    fprintf(out, "queue: %s\n", config->queue);
    fprintf(out, "routingKey: %s\n", config->routingKey);
    fprintf(out, "maxTransmitSize: %lu\n", config->maxTransmitSize);
    fprintf(out, "useSSL: %d\n", config->useSSL);
}

char *trim(char *string);

struct config *readConfig() {
    FILE *fp = fopen(CONFIG_FILE, "r");
    if (fp == NULL) return NULL;

    struct config *config = (struct config *) malloc(sizeof(struct config));
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
                config->servers = (char **) realloc(config->servers, sizeof(char *) * (server_cnt + 2));
                t_value = trim(t_value);
                if (t_value == NULL) continue;
                config->servers[server_cnt] = strdup(t_value);
                server_cnt++;
            }
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
            config->exchangeType = strdup(value);
        } else if (strcmp(key, "queue") == 0) {
            config->queue = strdup(value);
        } else if (strcmp(key, "routingKey") == 0) {
            config->routingKey = strdup(value);
        } else if (strcmp(key, "maxTransmitSize") == 0) {
            config->maxTransmitSize = strtoul(value, NULL, 10);
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
    *right = 0;
    return left;
}


void die_on_amqp_error(amqp_rpc_reply_t x, char const *context)
{
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
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
      fprintf(stderr, "%s: server connection error %d, message: %.*s\n",
              context,
              m->reply_code,
              (int) m->reply_text.len, (char *) m->reply_text.bytes);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
      fprintf(stderr, "%s: server channel error %d, message: %.*s\n",
              context,
              m->reply_code,
              (int) m->reply_text.len, (char *) m->reply_text.bytes);
      break;
    }
    default:
      fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
      break;
    }
    break;
  }

  exit(1);
}

char *select_server(struct config *config) {
    size_t n_servers = 0;
    char **ptr = config->servers;
    for ( ; ptr && *ptr; ptr++) {
        n_servers++;
    }

    if (n_servers == 0) return NULL;

    size_t idx = rand() % n_servers;
    char *server = config->servers[idx];

    /* swap last element with selected element */
    config->servers[idx] = config->servers[n_servers - 1];

    /* nullify final element (now the selected one) to shorten the list */
    config->servers[n_servers - 1] = NULL;
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
    char *server = select_server(config);

    int connected = 0;

    for ( ; server != NULL ; server = select_server(config) ) {
        printf( "Checking out %s:%d\n", server, config->port );
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

        status = amqp_socket_open(socket, server, config->port);
        if (status != 0) {
            continue;
        }
        connected = 1;
        break;
    }

    if (!connected) {
        fprintf(stderr, "Failed to connect to any servers!\n");
        exit(1);
    }

    reply = amqp_login(
            conn, config->vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
            config->username, config->password
    );
    die_on_amqp_error(reply, "login");

    amqp_channel_open(conn, 1);
    reply = amqp_get_rpc_reply(conn);
    die_on_amqp_error(reply, "channel open");

    amqp_exchange_declare(
            conn,
            1,
            amqp_cstring_bytes(config->exchange),
            amqp_cstring_bytes(config->exchangeType),
            0,
            1,
            0,
            0,
            amqp_empty_table
    );
    reply = amqp_get_rpc_reply(conn);
    die_on_amqp_error(reply, "exchange declare");

    message = "hello world";
    if (message != NULL) {
        amqp_bytes_t amqp_message;
        amqp_basic_properties_t message_props;

        amqp_message.len = strlen(message);
        amqp_message.bytes = message;
        /* TODO: figure out what these flags mean */
        message_props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG; 
        message_props.delivery_mode = 2;/* 1 or 2? */

        amqp_basic_publish(
                conn,                                   /* amqp_connection_state_t state */
                1,                                      /* amqp_channel_t channel */
                amqp_cstring_bytes(config->exchange),   /* amqp_bytes_t exchange */
                amqp_cstring_bytes(config->routingKey), /* amqp_bytes_t routing_key */
                0,                                      /* amqp_boolean_t mandatory */
                0,                                      /* amqp_boolean_t immediate */
                NULL,                                   /* amqp_basic_properties_t properties */
                amqp_message                            /* amqp_bytes_t body */
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
