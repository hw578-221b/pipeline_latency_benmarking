#ifndef PACKET_H // if PACKET_H has not been defined yet, continue compiling the following code
#define PACKET_H // if the same file is included again later, the compiler knows it has already seen this header

// every header file should protect itself against being included more than once. 
// This is especially important when headers include other headers.

#include <stdint.h>

// packet data structure
typedef struct {
    uint32_t seq;
    uint64_t t_schd_ns;
    uint64_t t_gen_ns;
    uint64_t t_cap_ns;
    uint64_t t_proc_start_ns;
    uint64_t t_proc_end_ns;
    uint64_t t_respond_ns;
    int payload;
    int cpu_gen, cpu_cap, cpu_proc, cpu_resp;
} packet_t;

#endif // This ends the conditional block
