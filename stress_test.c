// compile with gcc -Wall -Wextra stress_test.c -o stress_test

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

int main()
{
    // Generate data
    const unsigned arraySize = 32768;
    int data[arraySize];

    int count = 0;

    while(1) {
        for (unsigned c = 0; c < arraySize; ++c) {
            /* data[c] = std::rand() % 256; */
            data[c] = rand() % 256;
        }

        // printf ("size of data = %d, size of *data = %d\n", sizeof(data), sizeof(*data) );
        
        // // start a timer
        // struct timespec time1, time2;
        // clock_gettime(CLOCK_MONOTONIC, &time1);

        long long sum = 0;

        for (unsigned i = 0; i < 20000; ++i)
        {
            // Primary loop
            for (unsigned c = 0; c < arraySize; ++c)
            {
                if (data[c] >= 128)   //  add to sum only if element > 128
                    sum += data[c];
            }
        }

        // clock_gettime(CLOCK_MONOTONIC, &time2);
        // double elapsedTime = (time2.tv_sec - time1.tv_sec) + (time2.tv_nsec - time1.tv_nsec) / 1000000000.0;
        // printf ("elapsed time = %3.5fs\n", elapsedTime);

        count++;
    }
}
