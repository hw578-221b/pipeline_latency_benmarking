#include "shm_ipc_v2.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h> // for memset()

#include <unistd.h> // for close()
#include <fcntl.h> // for O_* constants
#include <sys/mman.h> // for mmap() function
#include <errno.h> // for perror(), errno
#include <signal.h>
#include <time.h>

#define NSEC_PER_SEC  1000000000
#define NSEC_PER_MSEC 1000000

static int shm_map(shm_ipc_t *ipc, const char* shm_name, const char* space_sem_name, const char* data_sem_name, bool create) {

    if(!ipc || !shm_name || !space_sem_name || !data_sem_name) {
        errno = EINVAL;
        perror("init_ipc");
        return -1;
    }

    // if shm_map() fails halfway through initialization, zeroing first means untouched fields are NULL,
    if(memset(ipc, 0, sizeof(*ipc)) == NULL) {
        perror("memset");
        return -1;
    }
    ipc->shm_name = shm_name;
    ipc->space_sem_name = space_sem_name;
    ipc->data_sem_name = data_sem_name;
    
    // Open side also need to have write access, can't be set to read only! since receiver also need to write to read_index in the buffer
    int O_constant;
    if(create) {
        O_constant = O_CREAT | O_RDWR;
    }
    else {
        O_constant = O_RDWR;
    }

    int shm_fd = shm_open(shm_name, O_constant, 0666);
    if(shm_fd == -1) {
        perror("shm_open");
        return -1;
    }

    if(create) {
        if(ftruncate(shm_fd, sizeof(shared_buffer_t)) == -1) {
            perror("ftruncate");
            close(shm_fd);
            return -1;
        }
    }

    ipc->buffer = mmap(NULL, sizeof(shared_buffer_t), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if(ipc->buffer == MAP_FAILED) {
        close(shm_fd);
        ipc->buffer = NULL;
        perror("mmap");
        return -1;
    }

    // Initalize ring buffer once at ring buffer creation after mmap succeeded
    if(create) {
        (ipc->buffer)->read_index = 0;
        (ipc->buffer)->write_index = 0;
    }

    if(create) {
        ipc->space_sem = sem_open(space_sem_name, O_CREAT, 0666, RING_SIZE);
        ipc->data_sem = sem_open(data_sem_name, O_CREAT, 0666, 0);
    }
    else {
        ipc->space_sem = sem_open(space_sem_name, 0);
        ipc->data_sem = sem_open(data_sem_name, 0);
    }

    if(ipc->data_sem == SEM_FAILED || ipc->space_sem == SEM_FAILED) {
        close(shm_fd);
        munmap(ipc->buffer, sizeof(shared_buffer_t));
        ipc->data_sem = NULL;
        ipc->space_sem = NULL;
        perror("sem_open");
        return -1;
    }

    // Need to be closed at the end to aviod fd leak!
    // When function exits, local variable shm_fd is gone, but the underlying file descriptor is still open inside the process
    close(shm_fd);

    return 0;
}

int shm_ipc_create(shm_ipc_t *ipc, const char* shm_name, const char* space_sem_name, const char* data_sem_name) {

    return shm_map(ipc, shm_name, space_sem_name, data_sem_name, 1);
}

int shm_ipc_open(shm_ipc_t *ipc, const char* shm_name, const char* space_sem_name, const char* data_sem_name) {

    return shm_map(ipc, shm_name, space_sem_name, data_sem_name, 0);
}

int shm_ipc_send(shm_ipc_t *ipc, packet_t* data) {

    if(!ipc || !ipc->buffer || !data) {
        errno = EINVAL;
        perror("send_init");
        return -1;
    }
    
    if(sem_wait(ipc->space_sem) == -1) {
        if(errno == EINTR) { // exit normally when interrupted by system signal
            return -1;
        }
        perror("sem_wait");
        return -1;
    }

    ipc->buffer->slots[ipc->buffer->write_index] = *data;
    ipc->buffer->write_index = (ipc->buffer->write_index + 1) % RING_SIZE; // for single sender only (without mutex)

    if(sem_post(ipc->data_sem) == -1) {
        perror("sem_post");
        return -1;
    }

    return 0;
}

int shm_ipc_recv(shm_ipc_t *ipc, packet_t* data, int timeout_ms) {

    struct timespec t;
    
    if(!ipc || !ipc->buffer || !data) {
        errno = EINVAL;
        perror("recv_init");
        return -1;
    }

    // Don't use CLOCK_MONOTONIC here! Since sem_timedwait() uses CLOCK_REALTIME (since 1970-01-01), if use CLOCK_MONOTONIC (since boot)
    // timestamp passed will aslo far in the past relative to CLOCK_REALTIME, so when no data is available
    // this funtion immediately returns ETIMEOUT, causing CPU cycles polling in event_cap() thread
    if(clock_gettime(CLOCK_REALTIME, &t) == -1) {
        perror("clock_gettime");
        return -1;
    }

    t.tv_nsec += timeout_ms * NSEC_PER_MSEC;
    while(t.tv_nsec >= NSEC_PER_SEC) {
        t.tv_nsec -= NSEC_PER_SEC;
        t.tv_sec++;
    }

    // Don't use sem_wait since it require sem_post to unblock and exit the thread, which makes thread read 1 staled data before exit
    if(sem_timedwait(ipc->data_sem, &t) == -1) {
        if(errno == ETIMEDOUT) {
            return -1;
        }
        else {
            perror("sem_timedwait");
            return -1;
        }
    }

    *data = ipc->buffer->slots[ipc->buffer->read_index];
    ipc->buffer->read_index = (ipc->buffer->read_index + 1) % RING_SIZE; // for single sender only (without mutex)

    if(sem_post(ipc->space_sem) == -1) {
        perror("sem_post");
        return -1;
    }

    return 0;
}

int shm_ipc_close(shm_ipc_t *ipc) {

    // Cleanup functions are usually better if they close whatever is valid and ignore nulls.
    int ret = 0;

    if(!ipc) {
        errno = EINVAL;
        perror("close_init");
        return -1;
    }

    if(ipc->buffer && ipc->buffer != MAP_FAILED) {
        if(munmap(ipc->buffer, sizeof(shared_buffer_t)) == -1) {
            perror("munmap");
            ret = -1;
        }
        ipc->buffer = NULL;
    }

    if(ipc->space_sem && ipc->space_sem != SEM_FAILED) {
        if(sem_close(ipc->space_sem) == -1) {
            perror("sem_close");
            ret = -1;
        }
        ipc->space_sem = NULL;
    }

    if(ipc->data_sem && ipc->data_sem != SEM_FAILED) {
        if(sem_close(ipc->data_sem) == -1) {
            perror("sem_close");
            ret = -1;
        }
        ipc->data_sem = NULL;
    }

    return ret;
}

int shm_ipc_unlink(const char* shm_name, const char* space_sem_name, const char* data_sem_name) {
    int ret = 0;

    if (!shm_name || !data_sem_name || !space_sem_name) {
        errno = EINVAL;
        perror("unlink_init");
        return -1;
    }

    if(shm_unlink(shm_name) == -1) {
        perror("shm_unlink");
        ret = -1;
    }

    if(sem_unlink(space_sem_name) == -1 || sem_unlink(data_sem_name) == -1) {
        perror("sem_unlink");
        ret = -1;
    }

    return ret;
}
