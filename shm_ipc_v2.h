#ifndef SHM_IPC_H
#define SHM_IPC_H

#include <semaphore.h>
#include <stdint.h>
#include "packet.h"

#define RING_SIZE 32

// shard memory structure (ring buffer)
typedef struct {
    int write_index;
    int read_index;
    packet_t slots[RING_SIZE];
} shared_buffer_t;

// data structure for interacting with shm_ipc functions
typedef struct {
    shared_buffer_t* buffer;
    sem_t* space_sem;
    sem_t* data_sem;
    const char* shm_name;
    const char* space_sem_name;
    const char* data_sem_name;
} shm_ipc_t;


// sender side open
int shm_ipc_create(shm_ipc_t *ipc, const char* shm_name, const char* space_sem_name, const char* data_sem_name);

// receiver side open
int shm_ipc_open(shm_ipc_t *ipc, const char* shm_name, const char* space_sem_name, const char* data_sem_name);

// sender side send data
int shm_ipc_send(shm_ipc_t *ipc, packet_t* data);

// receiver side receive data
int shm_ipc_recv(shm_ipc_t *ipc, packet_t* data, int timeout_ms);

// release the ipc handle/mappings
int shm_ipc_close(shm_ipc_t *ipc);

// delete the named ipc objects from system namespace
int shm_ipc_unlink(const char* shm_name, const char* space_sem_name, const char* data_sem_name);

#endif
