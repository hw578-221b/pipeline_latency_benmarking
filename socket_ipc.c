#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <sys/socket.h>
#include <sys/un.h>
#include "socket_ipc.h"

int listen_fd = -1, connect_fd = -1;

// server/client side funtion
int unix_socket_init(socket_ipc_t* ipc, int end_type) {

    if(!ipc) { // parameter checking
        errno = EINVAL;
        perror("socket_init");
        return -1;
    }

    // un means unix socket family
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr)); // initalize to 0

    // fill out the socket address
    addr.sun_family = AF_UNIX;
    // bound the copy by the destination field to ensure the '\0' termination when source is long
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    // if the caller is server side
    if(end_type == 1) {
        unlink(SOCKET_PATH); // clear staled path if present

        // creates an endpoint for communication and returns a file descriptor that refers to that endpoint
        ipc->listen_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
        if(ipc->listen_fd == -1) {
            perror("socket");
            return -1;
        }

        // Assign address specified by addr to socket referred by fd
        if(bind(ipc->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
            perror("bind");
            close(ipc->listen_fd);
            return -1;
        }
        // Mark the socket as passive socket waiting to accept incoming connection requests
        // 1 is the size of the wait queue for connection, here we only have 1 client (event_gen), so this can be 1
        if(listen(ipc->listen_fd, 1) == -1) {
            perror("listen");
            close(ipc->listen_fd);
            unlink(SOCKET_PATH);
            return -1;
        }

        // extracts the first connection request on the queue of pending connections for the listening socket, returns a new connected socket
        // Default fd(socket) type is blocking, so this function will block until receiving a connection request
        ipc->connect_fd = accept(ipc->listen_fd, NULL, NULL);
        if(ipc->connect_fd == -1) {
            perror("accept");
            close(ipc->listen_fd);
            unlink(SOCKET_PATH);
            return -1;
        }
    }
    // if the caller is client side
    else {
        int i = 0;

        while(1) {
            // create a new socket everytime if retry for portable applications 
            ipc->listen_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
            if(ipc->listen_fd == -1) {
                perror("socket");
                return -1;
            }

            // try connection
            if(connect(ipc->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
                break;
            }
            // if connection fails (due to server haven't started: socket haven't been bind to the pathname or haven't be in listening state
            else {
                i++;
            }
            
            // if reaching max trial times
            if(i == CONN_RETRY_LIMIT) {
                perror("connect");
                close(ipc->listen_fd);
                return -1;
            }

            close(ipc->listen_fd); // close the current socket before retry
            usleep(10000); // retry every 10ms
        }
    }

    return 0;
}

// server side function
int unix_socket_recv(socket_ipc_t* ipc, packet_t* packet) {

    if(!ipc || !packet) {
        errno = EINVAL;
        perror("recv_init");
        return -1;
    }

    // receive message from socket (no special flag needed)
    ssize_t n = recv(ipc->connect_fd, packet, sizeof(*packet), 0);
    if(n == -1) {
        perror("recv");
        return -1;
    }
    else if(n == 0) {
        fprintf(stderr, "Peer socket has closed!\n");
        return -1;
    }
    else if(n != sizeof(*packet)) {
        fprintf(stderr, "Short packet: got %zd bytes\n", n);
        return -1;
    }
    
    return 0;
}

// client side function
int unix_socket_send(socket_ipc_t* ipc, packet_t* packet) {

    if(!ipc || !packet) {
        errno = EINVAL;
        perror("send_init");
        return -1;
    }

    ssize_t n = send(ipc->listen_fd, packet, sizeof(*packet), 0); // use whatever fd returned by socket()
    if(n == -1) {
        perror("send");
        return -1;
    }
    else if (n != sizeof(*packet)) {
        fprintf(stderr, "Short packet: send %zd bytes\n", n);
        return -1;
    }

    return 0;
}

// server/client side funtion
int unix_socket_close(socket_ipc_t* ipc, int end_type) {

    int ret = 0;

    if(!ipc) {
        errno = EINVAL;
        perror("close_init");
        return -1;
    }
    
    if(close(ipc->listen_fd) == -1) {
        perror("close");
        ret = -1;
    }

    // if caller is server side
    if(end_type == 1) {
        if(close(ipc->connect_fd) == -1) {
            perror("close");
            ret = -1;
        }
        if(unlink(SOCKET_PATH) == -1) {
            perror("unlink");
            ret = -1;
        }
    }

    return ret;
}
