// compile with gcc -Wall -Wextra event_proc.c shm_ipc_v2.c socket_ipc.c -o event_proc -pthread -lrt
#define _GNU_SOURCE // for CPU_ZERO(), CPU_SET()

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

#include "shm_ipc_v2.h"
#include "socket_ipc.h"
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h> // for setpriority()
#include <sys/mman.h> // for mlockall()
#include <signal.h>

#define SHM_NAME "/front_shm"
#define SEM_NAME_SYNC "/sync_sem"
#define SEM_NAME_DATA "/data_sem"
#define SEM_NAME_SPACE "/space_sem"
#define NSEC_PER_SEC  1000000000
#define NSEC_PER_MSEC 1000000.0
#define NSEC_PER_USEC 1000.0

#define RECV_TIMEOUT 100 // in ms
#define QUEUE_SIZE 128
#define WARMUP_EVENTS 1

const char *event_log_path = "event_log.txt"; // default path (pointer changable, characters it points to unchangable)
int policy = SCHED_OTHER; // default policy
int priority = 0; // default priority
int pin = 0; // default thread CPU core pinning (unpinned)
uint64_t log_cap = 10000; // default capacity of events need to be recorded
uint64_t count = 0; // actual number of events processed
int ipc_select = 0; // default ipc mechanism (0 = shm, 1 = socket)

typedef struct {
    packet_t ring_buff[QUEUE_SIZE];
    sem_t data_sem;
    sem_t space_sem;
    pthread_mutex_t mutex; // optional here
    uint32_t write;
    uint32_t read;
} spsc_ring_t;

spsc_ring_t rbuff; // btw event_cap and event_proc
spsc_ring_t rbuff2; // btw event_proc and event_response

shm_ipc_t ipc;
socket_ipc_t ipc2;
sem_t *sync_sem;

static volatile sig_atomic_t stop_requested = 0;
FILE *event_log = NULL;

typedef struct {
    uint32_t latency1;
    uint32_t latency2;
    uint32_t latency3;
    uint32_t delay;
    int cpu_gen, cpu_cap, cpu_proc, cpu_resp;
} lat_log_t;

lat_log_t *latency_log = NULL;


static void stack_prefault(void) {
    volatile char stack[64 * 1024];
    
    for(size_t i=0; i < sizeof(stack); i += 4096) {
        stack[i] = 0;
    }

    return;
}

void* event_cap (void* arg) {
    stack_prefault();

    packet_t dpacket;
    struct timespec t; // Don's put this as a global variable since all 3 threads use it and this causes data race

    // zero the fields to prevent junk data inside
    if(memset(&dpacket, 0, sizeof(dpacket)) == NULL) {
        perror("memset");
        return NULL;
    }

    // Set the nice value if using default scheduling policy
    if(policy == SCHED_OTHER) {
        if(setpriority(PRIO_PROCESS, 0, priority) == -1) {
            perror("setpriority");
            return NULL;
        }
    }
 
    if(ipc_select == 0) {
        // event_gen.c runs before event_proc.c to create the named semaphore
        sync_sem = sem_open(SEM_NAME_SYNC, 0);
        if(sync_sem == SEM_FAILED) {
            perror("sem_open");
            return NULL;
        }

        // Singal the send process to start
        if(sem_post(sync_sem) == -1) {
            perror("sem_post");
            return NULL;
        }
    }
    else if(ipc_select == 1) {
        // This function will block until client (event_gen) send a connection request
        if(unix_socket_init(&ipc2, 1) == -1) {
            return NULL;
        }
    }

    uint64_t i = 0;
    
    while(!stop_requested) {

        /****************************************************************************************
        * recieve data from event_gen.c
        ****************************************************************************************/

        if(ipc_select == 0) {
            if(shm_ipc_recv(&ipc, &dpacket, RECV_TIMEOUT) == -1) {
                if(errno == ETIMEDOUT){
                    continue; // no data yet, skip to the top of loop and check stop_requested, retry if !stop_requested
                }
                fprintf(stderr, "shm_ipc_recv seq #%lu failed: %s", (unsigned long)i, strerror(errno));
                return NULL;
            }
        }
        else if(ipc_select == 1) {
            // this function will block until messages available in kernel socket buffer (for blocking socket)
            if(unix_socket_recv(&ipc2, &dpacket) == -1) {
                return NULL;
            }
        }

        if(clock_gettime(CLOCK_MONOTONIC, &t) == -1) {
            perror("clock_gettime");
            return NULL;
        }
        dpacket.t_cap_ns = (uint64_t)t.tv_sec * NSEC_PER_SEC + (uint64_t)t.tv_nsec;
        dpacket.cpu_cap = sched_getcpu();

        /****************************************************************************************
        * send data to event_proc thread
        ****************************************************************************************/
        
        // When buffer is not full, write data into it
        if(sem_wait(&rbuff.space_sem) == -1) {
            perror("space_sem_wait");
            return NULL;
        }
        // drop the current waiting data if exiting
        if(stop_requested) break;

        // // lock the critcal session with mutex (compatible to MPMC expansion)
        // if(pthread_mutex_lock(&rbuff.mutex) != 0) {
        //     perror("pthread_mutex_lock");
        //     return NULL;
        // }
        rbuff.ring_buff[rbuff.write] = dpacket;
        rbuff.write = (rbuff.write + 1) % QUEUE_SIZE;
        // //unlock the mutex
        // if(pthread_mutex_unlock(&rbuff.mutex) != 0) {
        //     perror("pthread_mutex_unlock");
        //     return NULL;
        // }

        if(sem_post(&rbuff.data_sem) == -1) { // notify consumer data available
            perror("data_sem_post");
            return NULL;
        }

        i++;
    }

    return NULL;
}

void* event_proc (void* arg) {
    stack_prefault();
    
    packet_t dpacket;
    struct timespec t;

    // zero the fields to prevent junk data inside
    if(memset(&dpacket, 0, sizeof(dpacket)) == NULL) {
        perror("memset");
        return NULL;
    }

    // Set the nice value if using default scheduling policy
    if(policy == SCHED_OTHER) {
        if(setpriority(PRIO_PROCESS, 0, priority) == -1) {
            perror("setpriority");
            return NULL;
        }
    }

    while(!stop_requested) {

        /****************************************************************************************
        * receive data from event_cap thread
        ****************************************************************************************/

        if(sem_wait(&rbuff.data_sem) == -1) {
            perror("data_sem_wait");
            return NULL;
        }
        // avoid processing staled data if exiting
        if(stop_requested) break;

        // // lock the critcal session with mutex (compatible to MPMC expansion)
        // if(pthread_mutex_lock(&rbuff.mutex) != 0) {
        //     perror("pthread_mutex_lock");
        //     return NULL;
        // }
        dpacket = rbuff.ring_buff[rbuff.read];
        rbuff.read = (rbuff.read + 1) % QUEUE_SIZE;
        // //unlock the mutex
        // if(pthread_mutex_unlock(&rbuff.mutex) != 0) {
        //     perror("pthread_mutex_unlock");
        //     return NULL;
        // }

        if(clock_gettime(CLOCK_MONOTONIC, &t) == -1) {
            perror("clock_gettime");
            return NULL;
        }
        dpacket.t_proc_start_ns = (uint64_t) t.tv_sec * NSEC_PER_SEC + (uint64_t) t.tv_nsec;
        dpacket.cpu_proc = sched_getcpu();

        if(sem_post(&rbuff.space_sem) == -1) {
            perror("space_sem_post");
            return NULL;
        }

        /****************************************************************************************
        * Process the data
        ****************************************************************************************/
        
        if(dpacket.payload > 50) {
            dpacket.payload += 100;
        }
        else {
            dpacket.payload -= 100;
        }

        if(clock_gettime(CLOCK_MONOTONIC, &t) == -1) {
            perror("clock_gettime");
            return NULL;
        }
        dpacket.t_proc_end_ns = (uint64_t) t.tv_sec * NSEC_PER_SEC + (uint64_t) t.tv_nsec;

        /****************************************************************************************
        * Send to event_response thread
        ****************************************************************************************/

        // When buffer is not full, write data into it
        if(sem_wait(&rbuff2.space_sem) == -1) {
            perror("space_sem_wait");
            return NULL;
        }
        // drop the current waiting data if exiting
        if(stop_requested) break;

        // // lock the critcal session with mutex (compatible to MPMC expansion)
        // if(pthread_mutex_lock(&rbuff2.mutex) != 0) {
        //     perror("pthread_mutex_lock");
        //     return NULL;
        // }
        rbuff2.ring_buff[rbuff2.write] = dpacket;
        rbuff2.write = (rbuff2.write + 1) % QUEUE_SIZE;
        // //unlock the mutex
        // if(pthread_mutex_unlock(&rbuff2.mutex) != 0) {
        //     perror("pthread_mutex_unlock");
        //     return NULL;
        // }

        if(sem_post(&rbuff2.data_sem) == -1) {
            perror("data_sem_post");
            return NULL;
        }
    }

    return NULL;
}

void * event_resp (void * arg) {
    stack_prefault();

    packet_t dpacket;
    struct timespec t;

    // zero the fields to prevent junk data inside
    if(memset(&dpacket, 0, sizeof(dpacket)) == NULL) {
        perror("memset");
        return NULL;
    }

    // Set the nice value if using default scheduling policy
    if(policy == SCHED_OTHER) {
        if(setpriority(PRIO_PROCESS, 0, priority) == -1) {
            perror("setpriority");
            return NULL;
        }
    }

    // // print scheduling parameters for debug!
    // int cur_policy = -1, nice_value = 999;
    // struct sched_param sp;
    // cur_policy = sched_getscheduler(0); // 0 = calling thread
    // sched_getparam(0, &sp);
    // nice_value = getpriority(PRIO_PROCESS, 0);

    // printf("event_proc threads scheduling policy: %d, priority: %d, nice value: %d\n", cur_policy, sp.sched_priority, nice_value);

    while(!stop_requested) {
        
        /****************************************************************************************
        * receive data from event_proc thread
        ****************************************************************************************/

        if(sem_wait(&rbuff2.data_sem) == -1) {
            perror("data_sem_wait");
            return NULL;
        }
        // avoid processing staled data if exiting
        if(stop_requested) break;

        // // lock the critcal session with mutex (compatible to MPMC expansion)
        // if(pthread_mutex_lock(&rbuff2.mutex) != 0) {
        //     perror("pthread_mutex_lock");
        //     return NULL;
        // }
        dpacket = rbuff2.ring_buff[rbuff2.read];
        rbuff2.read = (rbuff2.read + 1) % QUEUE_SIZE;
        // //unlock the mutex
        // if(pthread_mutex_unlock(&rbuff2.mutex) != 0) {
        //     perror("pthread_mutex_unlock");
        //     return NULL;
        // }

        if(clock_gettime(CLOCK_MONOTONIC, &t) == -1) {
            perror("clock_gettime");
            return NULL;
        }
        dpacket.t_respond_ns = (uint64_t) t.tv_sec * NSEC_PER_SEC + (uint64_t) t.tv_nsec;
        dpacket.cpu_resp = sched_getcpu();

        if(sem_post(&rbuff2.space_sem) == -1) {
            perror("space_sem_post");
            return NULL;
        }

        // Don't print out data in the real-time loop as this file I/O delay could be a part of latency3!!!!!!!!!!!!!!!
        latency_log[count].latency1 = dpacket.t_cap_ns - dpacket.t_gen_ns;
        latency_log[count].latency2 = dpacket.t_proc_start_ns - dpacket.t_cap_ns;
        latency_log[count].latency3 = dpacket.t_respond_ns - dpacket.t_proc_end_ns;
        latency_log[count].delay = dpacket.t_gen_ns - dpacket.t_schd_ns;

        latency_log[count].cpu_gen = dpacket.cpu_gen;
        latency_log[count].cpu_cap = dpacket.cpu_cap;
        latency_log[count].cpu_proc = dpacket.cpu_proc;
        latency_log[count].cpu_resp = dpacket.cpu_resp;

        count++;

        // prevent overflow, stop the process when events exceeding the capacity of allocated buffer!!!!!!!!!!!!
        if(count >= log_cap + 1000) {
            stop_requested = 1;
            break;
        }
        
        // if(dpacket.seq >= WARMUP_EVENTS)
        //     fprintf(event_log, "%lu %lu %lu %lu\n", latency1, latency2, latency3, delay);
    }
    
    return NULL;
}

int main(int argc, char* argv[]) {
    char *end;

    // command line parsing
    if(argc >= 2) {
        event_log_path = argv[1];
    }
    if(argc >= 3) {
        if(strcmp(argv[2], "fifo") == 0) {
            policy = SCHED_FIFO;
        }
        else if(strcmp(argv[2], "rr") == 0) {
            policy = SCHED_RR;
        }
        else if(strcmp(argv[2], "other") == 0) {
            policy = SCHED_OTHER;
        }
        else {
            fprintf(stderr, "Invalid policy!\n");
            return -1;
        }
    }
    if(argc >= 4) {
        errno = 0;
        priority = (int)strtol(argv[3], &end, 10);
        if(errno == EINVAL || errno == ERANGE) {
            perror("strtol");
            return -1;
        }
        // value error checking is performed in python (calling script), so no error checking here
        if(*end != '\0') {
            fprintf(stderr, "Invalid priority!\n");
            return -1;
        }
    }
    if(argc >= 5) {
        errno = 0;
        log_cap = (uint64_t)strtoul(argv[4], NULL, 10);
        if(errno == EINVAL || errno == ERANGE) {
            perror("strtol");
            return -1;
        }
    }
    if (argc >= 6) {
        // value error checking is performed in python (calling script), so no error checking here
       if(strcmp(argv[5], "shm") == 0) {
            ipc_select = 0;
        }
        else if(strcmp(argv[5], "socket") == 0) {
            ipc_select = 1;
        }
        else {
            fprintf(stderr, "Invalid ipc parameters\n");
            return -1;
        }
    }
    if(argc >= 7) {
        errno = 0;
        pin = (int)strtol(argv[6], &end, 10);
        if(errno == EINVAL || errno == ERANGE) {
            perror("strtol");
            return -1;
        }
        if(*end != '\0' || (pin != 0 && pin != 1)) {
            fprintf(stderr, "Invalid thread core pinning parameters!\n");
            return -1;
        }
    }

    // Pre-allocate the memory buffer according to the capacity of events that need to be processed
    latency_log = calloc(log_cap + 1000, sizeof(lat_log_t));
    if(!latency_log){
        perror("calloc");
        return -1;
    }

    int ret = 0, sig;
    sigset_t set; // sigset_t is a type that stores a group of signals
    pthread_t capture_thread, process_thread, response_thread;

    // Initalize rbuff struct
    rbuff.read = 0;
    rbuff.write = 0;

    if(sem_init(&rbuff.data_sem, 0, 0) == -1) {
        perror("data_sem_init");
        return -1;
    }
    if(sem_init(&rbuff.space_sem, 0, QUEUE_SIZE) == -1) {
        perror("space_sem_init");
        return -1;
    }
    if(pthread_mutex_init(&rbuff.mutex, NULL) != 0) {
        perror("pthread_mutex_init");
        return -1;
    }

    // Initalize rbuff2 struct
    rbuff2.read = 0;
    rbuff2.write = 0;

    if(sem_init(&rbuff2.data_sem, 0, 0) == -1) {
        perror("data_sem_init");
        return -1;
    }
    if(sem_init(&rbuff2.space_sem, 0, QUEUE_SIZE) == -1) {
        perror("space_sem_init");
        return -1;
    }
    if(pthread_mutex_init(&rbuff2.mutex, NULL) != 0) {
        perror("pthread_mutex_init");
        return -1;
    }

    // Open the log file to store the output of event_proc
    event_log = fopen(event_log_path, "w");
    if(!event_log) {
        perror("fopen");
        return -1;
    }

    if(ipc_select == 0) {
        // Initalize shared memory IPC
        if(shm_ipc_open(&ipc, SHM_NAME, SEM_NAME_SPACE, SEM_NAME_DATA) == -1) {
            fclose(event_log);
            perror("shm_ipc_open");
            return -1;
        }
    }

    // Lock the memory to prevent page fault and memory swapping
    if(mlockall(MCL_CURRENT | MCL_FUTURE) == -1) {
        perror("mlockall");
        return -1;
    }

    pthread_attr_t attr;
    struct sched_param sp;
    memset(&sp, 0, sizeof(sp));

    // init the thread attributes object with default attribute values
    int rc = pthread_attr_init(&attr);
    if(rc != 0) {
        errno = rc;
        perror("pthread_attr_init");
        return -1;
    }
    // set scheduling policy
    rc = pthread_attr_setschedpolicy(&attr, policy);
    if(rc != 0) {
        errno = rc;
        perror("pthread_attr_setschedpolicy");
        return -1;
    }
    // set scheduling priority
    if(policy == SCHED_OTHER)
        sp.sched_priority = 0;
    else
        sp.sched_priority = priority;
        
    rc = pthread_attr_setschedparam(&attr, &sp);
    if(rc != 0) {
        errno = rc;
        perror("pthread_attr_setschedparam");
        return -1;
    }
    // use the values in attr rather than inheriting the creating thread's scheduling policy and priority
    rc = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    if(rc != 0) {
        errno = rc;
        perror("pthread_attr_setinheritsched");
        return -1;
    }

    sigemptyset(&set); // Initialize it empty
    sigaddset(&set, SIGINT); // Add Ctrl+C signal
    sigaddset(&set, SIGTERM); // normal termination request: kill <pid>

    // Set the signal mask and block it in the future new threads
    // Signal masks are inherited by new threads. So if you call this before pthread_create(), the worker thread also starts with SIGINT blocked.
    if(pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
        perror("pthread_sigmask");
        return -1;
    }

    // Order matters here!!! Downstream consumers should already alive and blocked before the upstream producer starts feeding them
    pthread_create(&response_thread, &attr, event_resp, NULL);
    pthread_create(&process_thread, &attr, event_proc, NULL);
    pthread_create(&capture_thread, &attr, event_cap, NULL);

    // cpu affinity set, these should be called after pthread_create()!!!!!
    if(pin) {
        cpu_set_t set1, set2, set3;
        CPU_ZERO(&set1);
        CPU_ZERO(&set2);
        CPU_ZERO(&set3);
        CPU_SET(2, &set1);
        CPU_SET(3, &set2);
        CPU_SET(0, &set3);
        rc = pthread_setaffinity_np(capture_thread, sizeof(set1), &set1); // pin to CPU 2
        if(rc != 0) {
            fprintf(stderr, "capture thread affinity: %s\n", strerror(rc));
            return -1;
        }
        rc = pthread_setaffinity_np(process_thread, sizeof(set2), &set2); // pin to CPU 3
        if(rc != 0) {
            fprintf(stderr, "process thread affinity: %s\n", strerror(rc));
            return -1;
        }
        rc = pthread_setaffinity_np(response_thread, sizeof(set3), &set3); // pin to CPU 0
        if(rc != 0) {
            fprintf(stderr, "response thread affinity: %s\n", strerror(rc));
            return -1;
        }
    }

    // Wait for the signal in main, store the received signal number in sig
    rc = sigwait(&set, &sig);
    if(rc != 0) { // sigwait() does not return -1 and set errno like many POSIX functions. It returns the error code directly.
        fprintf(stderr, "sigwait: %s\n", strerror(rc));
    }

    stop_requested = 1;

    sem_post(&rbuff.data_sem);   // wake event_proc if queue is empty
    sem_post(&rbuff.space_sem);  // wake event_cap if queue is full

    sem_post(&rbuff2.data_sem);
    sem_post(&rbuff2.space_sem); 
    // A mutex must be unlocked by the same thread that locked it, so no mutex unlock here
    
    pthread_join(capture_thread, NULL);
    pthread_join(process_thread, NULL);
    pthread_join(response_thread, NULL);

    // print out all the data to event log
    // only log the data when system stablizes (exclude the initial few data points)
    for(uint64_t i = WARMUP_EVENTS; i < count; i++) {
        fprintf(event_log, "%u %u %u %u %u %u %u %u\n", 
            latency_log[i].latency1, latency_log[i].latency2, latency_log[i].latency3, latency_log[i].delay,
            latency_log[i].cpu_gen, latency_log[i].cpu_cap, latency_log[i].cpu_proc, latency_log[i].cpu_resp);
    }

    /****************************************************************************************
    * Clean up code 
    ****************************************************************************************/

    if(fclose(event_log) == EOF) {
        perror("fclose");
        ret = -1;
    }

    free(latency_log);
    latency_log = NULL;

    if(sem_destroy(&rbuff.data_sem) == -1) {
        perror("data_sem_destroy");
        ret = -1;
    }
    if(sem_destroy(&rbuff.space_sem) == -1) {
        perror("space_sem_destroy");
        ret = -1;
    }
    if(pthread_mutex_destroy(&rbuff.mutex) != 0) {
        perror("pthread_mutex_destroy");
        ret = -1;
    }

    if(sem_destroy(&rbuff2.data_sem) == -1) {
        perror("data_sem_destroy");
        ret = -1;
    }
    if(sem_destroy(&rbuff2.space_sem) == -1) {
        perror("space_sem_destroy");
        ret = -1;
    }
    if(pthread_mutex_destroy(&rbuff2.mutex) != 0) {
        perror("pthread_mutex_destroy");
        ret = -1;
    }

    if(ipc_select == 0) {
        if(sem_close(sync_sem) == -1) {
            perror("sem_close");
            ret = -1;
        }
        if(sem_unlink(SEM_NAME_SYNC) == -1) {
            perror("sem_unlink");
            ret = -1;
        }

        if(shm_ipc_close(&ipc) == -1) {
            perror("shm_ipc_close");
            ret = -1;
        }
        // Put this in the process that finishes last
        if(shm_ipc_unlink(SHM_NAME, SEM_NAME_SPACE, SEM_NAME_DATA) == -1) {
            perror("shm_ipc_unlink");
            ret = -1;
        }
    }
    else if (ipc_select == 1) {
        if(unix_socket_close(&ipc2, 1) == -1) {
            ret = -1;
        }   
    }

    printf("event_proc.c exited cleanly!\n");

    return ret;
}
