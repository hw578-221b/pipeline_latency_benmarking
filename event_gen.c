// compile with gcc -Wall -Wextra event_gen.c shm_ipc_v2.c socket_ipc.c -o event_gen  -pthread -lrt
#define _GNU_SOURCE // for CPU_ZERO(), CPU_SET()

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h> // close(), read()
#include <string.h>
#include <time.h>
#include <stdbool.h>

#include "shm_ipc_v2.h"
#include "socket_ipc.h"
#include <fcntl.h> // for O_* constants, open()
#include <errno.h>
#include <signal.h>
#include <sched.h>
#include <sys/resource.h> // for setpriority()
#include <sys/mman.h> // for mlockall()
#include <pigpio.h>

#define SHM_NAME "/front_shm"
#define SEM_NAME_SYNC "/sync_sem"
#define SEM_NAME_DATA "/data_sem"
#define SEM_NAME_SPACE "/space_sem"
#define NSEC_PER_SEC  1000000000
#define NSEC_PER_USEC 1000

// defined in stdlib.h, used by getopt() to store the argument value of the current option character being processed
extern char* optarg;

packet_t dpacket;
shm_ipc_t ipc; // don't use global pointer, it's initialized to NULL (will fail the parameter check inside function)!
socket_ipc_t ipc2;

struct timespec t;
struct timespec now;

sem_t* sync_sem;
static volatile sig_atomic_t stop_requested = 0;

// mutex configuration is removed for inter thread latency
// add stack pre-fault and memory-locking
// integrate set cpu governor script into python automation
// add socket IPC and complete policy and priority setting for event_proc.c
// add stress test mode and cpu pinnning mode
// add a help command in pipeline.py to help user know the options

static void signal_handler(int sig) {
    stop_requested = 1;
}

int main(int argc, char *argv[]) { // argc: number of arguments， *argv[]: array of pointers (undetermined size) to char

    int i = 0, ddl_miss = 0, opt;
    int gen_gap_us = 1000000, priority = 0; // default value
    const char* policy = "other"; // default value
    int ipc_select = 0; // default ipc mechanism (0 = shm, 1 = socket)
    int pin = 0; // default thread CPU core pinning (unpinned)
    char* end;

    struct sched_param param;
    memset(&param, 0, sizeof(param)); // set this to 0 for safe initalization

    /****************************************************************************************
    * Command line arguement parsing
    ****************************************************************************************/

    while((opt = getopt(argc, argv, "g:s:p:i:")) != -1) { // keep finding the option characters until reaching the end of arguement
        switch(opt) {
            case 'g':
                errno = 0; // reset errno before calling strtol()
                gen_gap_us = (int)strtol(optarg, &end, 10); // safer than atoi()
                if(errno == ERANGE || errno == EINVAL) {
                    perror("strtol");
                    return -1;
                }
                // fist check if int type, python argparse will also perform type check to ensure!!!
                // then check if value is negative or 0!(python also check), empty case will be checked by "g:" in getopt()
                if(*end != '\0' || gen_gap_us <= 0) {
                    fprintf(stderr, "Invalid integer: %s\n", optarg);
                    return -1;
                }
                break;
            
            case 's':
                // value checking will be performed in python, so no error checking here!!!!
                policy = optarg;
                break;
            
            case 'p':
                errno = 0;
                // value range checking will be performed in python, so no error checking here!!!!
                priority = (int)strtol(optarg, &end, 10);
                if(errno == EINVAL || errno == ERANGE) {
                    perror("strtol");
                    return -1;
                }
                break;
            
            case 'i':
                // value checking will be performed in python, so no error checking here!!!!
                if(strcmp(optarg, "shm") == 0) {
                    ipc_select = 0;
                }
                else if(strcmp(optarg, "socket") == 0) {
                    ipc_select = 1;
                }
                else {
                    fprintf(stderr, "Invalid ipc parameters\n");
                    return -1;
                }
                break;

            default:
                fprintf(stderr, "Invalid command\n");
                return -1;
        }
    }

    /****************************************************************************************
    * Initialization code 
    ****************************************************************************************/

    // get thread core pinning configuration
    if(argc >= 10) {
        errno = 0;
        pin = (int)strtol(argv[9], &end, 10);
        if(errno == ERANGE || errno == EINVAL) {
            perror("strtol");
            return -1;
        }
        if(*end != '\0' || (pin != 0 && pin != 1)) {
            fprintf(stderr, "Invalid thread core pinning parameter!\n");
            return -1;
        }
    }

    // Set policy and priority together after command line arguement parsing
    if(strcmp(policy, "fifo") == 0) { // if policy input is fifo
        param.sched_priority = priority;
        if(sched_setscheduler(0, SCHED_FIFO, &param) == -1) { // 0 means current calling process
            perror("sched_setscheduler");
            return -1;
        }
    }
    else if (strcmp(policy, "rr") == 0) { // if policy input is rr
        param.sched_priority = priority;
        if(sched_setscheduler(0, SCHED_RR, &param) == -1) {
            perror("sched_setscheduler");
            return -1;
        }
    }
    else if (strcmp(policy, "other") == 0) { // if policy input is other (default)
        if(setpriority(PRIO_PROCESS, 0, priority) == -1) {
            perror("setpriority");
            return -1;
        }
    }
    else {
        fprintf(stderr, "Invalid policy parameters: %s\n", policy);
        return -1;
    }

    // set CPU affinity
    if(pin == 1) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(1, &set); // pin to CPU 1
        if(sched_setaffinity(0, sizeof(set), &set) == -1) {
            perror("sched_setaffinity");
            return -1;
        }
    }

    if(strcmp(policy, "rr") == 0) {
        struct timespec tp;
        if(sched_rr_get_interval(0, &tp) == -1) {
            perror("sched_rr_get_interval");
            return -1;
        }
        float interval = tp.tv_sec * 1000 + tp.tv_nsec / 1000000.0;
        printf("Round robin time quantum: %.2f ms\n", interval);
    }

    // // print scheduling parameters for debug!
    // int test_policy, nice_value;
    // struct sched_param sp;
    // test_policy = sched_getscheduler(0); // 0 = calling thread
    // sched_getparam(0, &sp);
    // nice_value = getpriority(PRIO_PROCESS, 0);

    // printf("event_gen scheduling policy: %d, priority: %d, nice value: %d\n", test_policy, sp.sched_priority, nice_value);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa)); // safe initalization
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask); // no signal mask, so main thread can receive signals normally
    sa.sa_flags = 0; // no flags to modify the behavior of the signal

    if(sigaction(SIGINT, &sa, NULL) == -1) {
        perror("sigaction-SIGINT");
        return -1;
    }
    if(sigaction(SIGTERM, &sa, NULL) == -1) {
        perror("sigaction-SIGTERM");
        return -1;
    }

    // Use an always changing seed (system time) so that rand() generate different sequence of random number each time it runs
    srand((unsigned int)time(NULL));

    // Lock the memory to prevent page fault and memory swapping
    if(mlockall(MCL_CURRENT | MCL_FUTURE) == -1) {
        perror("mlockall");
        return -1;
    }
    
    // init code for shared memory ipc
    if(ipc_select == 0) {
        if(shm_ipc_create(&ipc, SHM_NAME, SEM_NAME_SPACE, SEM_NAME_DATA) == -1) {
            perror("shm_ipc_create");
            return -1;
        }

        sync_sem = sem_open(SEM_NAME_SYNC, O_CREAT, 0666, 0);
        if(sync_sem == SEM_FAILED) {
            perror("sem_open");
            return -1;
        }

        // Wait for recv process's signal before start data generation & transmission
        if(sem_wait(sync_sem) == -1) {
            if(errno == EINTR && stop_requested == 1) { // exit normally if interrupted by system signal
                int ret = 0;
                // process clean up (close and unlink)
                if(shm_ipc_close(&ipc) == -1) {
                    perror("shm_ipc_close");
                    ret = -1;
                }
                if(shm_ipc_unlink(SHM_NAME, SEM_NAME_SPACE, SEM_NAME_DATA) == -1) {
                    perror("shm_ipc_unlink");
                    ret = -1;
                }
                if(sem_close(sync_sem) == -1) {
                    perror("sem_close");
                    ret = -1;
                }
                if(sem_unlink(SEM_NAME_SYNC) == -1) {
                    perror("sem_unlink");
                    ret = -1;
                }
                printf("\n event_gen exited cleanly before sync with event_proc!\n");
                return ret;
            }

            perror("sem_wait");
            return -1;
        }
    }
    // init code for unix domain socket ipc
    else if(ipc_select == 1) {
        // Initalize the unix domain socket (block/retry until server side is set up and connection established)
        if(unix_socket_init(&ipc2, 0) == -1) {
            return -1;
        }
    }

    // start the timer for fixed period generation
    if(clock_gettime(CLOCK_MONOTONIC, &t) == -1) {
        perror("clock_gettime");
        return -1;
    }

    /****************************************************************************************
    * Main event generation loop
    ****************************************************************************************/

    // absolute-time design, it does not accumulate extra delay just because one iteration ran long
    while (!stop_requested) {

        dpacket.seq = i;
        dpacket.payload = rand() % 100; // 0-99 random number
        if(clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
            perror("clock_gettime");
            return -1;
        }
        // difference btw schedule and actual send time is mostly due to the wake up delay of clock_nanosleep()
        dpacket.t_schd_ns = (uint64_t)t.tv_sec * NSEC_PER_SEC + (uint64_t)t.tv_nsec;
        dpacket.t_gen_ns = (uint64_t)now.tv_sec * NSEC_PER_SEC + (uint64_t)now.tv_nsec;
        i++;
        dpacket.cpu_gen = sched_getcpu();

        if(ipc_select == 0) {
            if(shm_ipc_send(&ipc, &dpacket) == -1) {
                if(errno == EINTR && stop_requested == 1) { // exit normally when interrupted by system signal!!!
                    break;
                }
                fprintf(stderr, "shm_ipc_send seq #%d failed: %s\n", i, strerror(errno));
                return -1;
            }
        }
        else if(ipc_select == 1) {
            if(unix_socket_send(&ipc2, &dpacket) == -1) {
                if(errno == EINTR && stop_requested == 1) { // exit normally when interrupted by system signal
                    break;
                }
                return -1;
            }
        }

        // Schedule the next deadline (Send the data packet every GEN_GAP_US)
        t.tv_nsec += gen_gap_us * NSEC_PER_USEC;
        // Accounts for 1 sec rollover
        while(t.tv_nsec >= NSEC_PER_SEC) {
            t.tv_nsec -= NSEC_PER_SEC;
            t.tv_sec++;
        }

        // Deadline miss detection
        if(clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
            perror("clock_gettime");
            return -1;
        }
        if(now.tv_sec > t.tv_sec || (now.tv_sec == t.tv_sec && now.tv_nsec > t.tv_nsec)) {
            ddl_miss++; // count the times of deadline miss
            continue;
        }

        // Sleep unitl next deadline, or skip sleeping if deadline is missed
        int rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &t, NULL);
        if(rc != 0) {
            if(rc == EINTR && stop_requested == 1) { // exit normally when interrupted by system signal!!!
                break;
            }
            errno = rc;
            perror("clock_nanosleep");
            return -1;
        }
    }
    
    /****************************************************************************************
    * Clean up code 
    ****************************************************************************************/
    
    int ret2 = 0;

    if(ipc_select == 0) {
        // Don't do unlink here, otherwise another process opens shm can fail (name is gone) if unlink is executed before it did 
        // Put unlink in the process that finishes last
        if(shm_ipc_close(&ipc) == -1) {
            perror("shm_ipc_close");
            ret2 = -1;
        }
        if(sem_close(sync_sem) == -1) {
            perror("sem_close");
            ret2 = -1;
        }
    }
    else if (ipc_select == 1) {
        if(unix_socket_close(&ipc2, 0) == -1) {
            ret2 = -1;
        }
    }

    printf("Missed generation deadline events: %d\n", ddl_miss);
    printf("event_gen.c exited cleanly!\n");

    return ret2;
}
