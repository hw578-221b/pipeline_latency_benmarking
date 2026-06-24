#ifndef SOCKET_IPC_H
#define SOCKET_IPC_H

#include "packet.h"

#define SOCKET_PATH "/tmp/pipeline.sock"
#define CONN_RETRY_LIMIT 500

typedef struct {
    int listen_fd;
    int connect_fd;
} socket_ipc_t;

// server/client side funtion
int unix_socket_init(socket_ipc_t* ipc, int end_type);

// server side function
int unix_socket_recv(socket_ipc_t* ipc, packet_t* packet);

// client side function
int unix_socket_send(socket_ipc_t* ipc, packet_t* packet);

// server/client side funtion
int unix_socket_close(socket_ipc_t* ipc, int end_type);

#endif
