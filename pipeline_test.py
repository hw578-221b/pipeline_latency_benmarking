#!/usr/bin/env python3
#This is a Unix script launcher hint. It tells the OS run this file using python3 found in $PATH

import argparse     # command-line argument parsing
import os           # OS operations like file deletion, unlink
import signal       # singal module, signal constants like SIGINT
import subprocess   # start and manage child processes
import sys          # sys.exit()
import time         # sleep()
import glob         # find pathnames

import latency_plot # latency data processing and plotting

# subprocess object
gen_proc = None
proc_proc = None

prev_governors = {} # empty dictionary storing the previously used cpu scaling governor

# send signal to the process, if process already exited, do nothing
def proc_signal(proc, sig):
    if proc is None:
        return
    # .poll() checks whether the child process has already exited
    # return NONE is still running, if already finished return integer exit code
    if proc.poll() is None: 
        try:
            proc.send_signal(sig)
        except ProcessLookupError:
            pass # means ignore it, do nothing

# wait for the process to exit with timeout
# function with a default parameter, if caller does proc_wait(gen_proc), then time becomes None
def proc_wait(proc, time=None):
    if proc is None:
        return True
    try:
        proc.wait(timeout=time) # keyword argument
        return True
    except subprocess.TimeoutExpired:
        return False

def stop_processes():
    global gen_proc, proc_proc

    proc_signal(gen_proc, signal.SIGINT)
    proc_signal(proc_proc, signal.SIGINT)

    if not proc_wait(gen_proc, time=10):
        # send the printed text to stderr instead of stdout (print as error message)
        print("event_gen did not exit cleanly after SIGINT", file=sys.stderr)
        sys.exit(1)
    # check the return code so that you can distinguish a normal exited or unexpected exit like program crash!!!!!!!!!!!!!!!!!!!!!
    if gen_proc.returncode != 0: # type: ignore
        print(f"event_gen.c exited with return code {gen_proc.returncode}", file=sys.stderr) # type: ignore

    if not proc_wait(proc_proc, time=10):
        print("event_proc did not exit cleanly after SIGINT", file=sys.stderr)
        sys.exit(1)
    if proc_proc.returncode != 0: # type: ignore
        print(f"event_proc.c exited with return code {proc_proc.returncode}", file=sys.stderr) # type: ignore

# backup cleanup function   
def ipc_cleanup():
    try:
        for path in [
            "/dev/shm/front_shm",
            "/dev/shm/sem.data_sem",
            "/dev/shm/sem.space_sem",
            "/dev/shm/sem.sync_sem",
            "/tmp/pipeline.sock"
        ]:
            os.unlink(path)
    except FileNotFoundError: # try to remove it; if already gone, that's fine
        pass

# clean up function (signal handler) when signal is captured
# signal handler variable (not used here): signum: signal number of the signal received, frame: current stack frame object
def cleanup(signum=None, frame=None):

    stop_processes()
    ipc_cleanup()

    sys.exit(0)


def set_cpu_governors(governor="performance"):
    global prev_governors
    paths = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"))

    if not paths:
        print("No scaling governor file found at designated location. Skipping gorvernor setup.", file=sys.stderr)
        return
    
    # save previous governor to restore them when pipeline finished
    for path in paths:
        try:
            with open(path, "r") as f:
                prev_governors[path] = f.read().strip()
        except OSError as e:
            raise OSError(f"Cannot read {path}: {e}.")

    # set new governer
    for path in paths:
        try:
            with open(path, "w") as f:
                f.write(f"{governor}\n")
        except PermissionError:
            raise PermissionError(f"Permission denied when writing {path}. Run this script with sudo")
        except OSError as e:
            raise OSError(f"Fail to set governor for {path}: {e}.")
    
    # verify
    for path in paths:
        try:
            with open(path, "r") as f:
                actual = f.read().strip()
            if actual != governor:
                f"Governor verification failed for {path}: expected {governor}, got {actual}"
        except OSError as e:
            raise OSError(f"Fail to verify governor for {path}: {e}.")
    
    print(f"CPU scaling governor set to {governor} for {len(paths)} CPU entries.")


def restore_cpu_governors():
    global prev_governors

    if not prev_governors:
        raise ValueError("Invalid saved governors!")
    
    for path, governor in prev_governors.items():
        try:
            with open(path, "w") as f:
                f.write(governor + "\n")
        except OSError as e:
            raise OSError(f"Fail to restore governor for {path}: {e}.")
    
    print("CPU scaling governors restored.")


# run pipline and analysis once
def run_pipeline(run_index, runtime, gap, policy, priority, ipc, pin, stress, event_log_path, do_plot=True):
    global gen_proc, proc_proc
    stress_procs = [] # empty list

    # argument validation check
    if runtime <= 0:
        raise ValueError("runtime must be positive")
    if gap <= 0:
        raise ValueError("gap must be positive")
    elif gap <= 10:
        raise ValueError("gap must be greater than 10 us")

    policy_parameters = {  # python directionary syntax
        "other": (-20, 19),
        "fifo": (1, 99),
        "rr": (1, 99)
    }
    if policy not in policy_parameters:
        raise ValueError("invalid policy parameter (available parameters: other, fifo, rr)")
    
    min_prio, max_prio = policy_parameters[policy]
    if not (min_prio <= priority <= max_prio):
        raise ValueError(
            f"invalid priority for policy {policy}: "
            f"must be between {min_prio} and {max_prio}"
        )
    
    # if in stress mode, start 20 copies of stress_test
    if stress:
        for _ in range(20):
            stress_procs.append(
                subprocess.Popen(["./stress_test"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            )
        print("Started 20 copies of stress_test (sorting) processes!")

    # in order for CPU to keep at max frequency during tests
    set_cpu_governors("performance")

    # Register signal handler of this python script to be cleanup function
    # The first signal is the module name, the second .signal(...) is the function inside
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    print("Latency benchmarking started...")

    # starts a child process and returns immediately
    #["./event_gen"] This is the command and its arguments (passed with ",")
    gen_proc = subprocess.Popen(["./event_gen", "-g", str(gap), "-s", policy, "-p", str(priority), "-i", ipc, str(int(pin))]) # if use str(pin) directly, will become "True"/"False" string
    # Gives event_gen time to create IPC resources before event_proc starts
    time.sleep(0.1)
    # calculate how many events will be generated during runtime and pass to event_proc for pre-allocating memory buffer
    data_ctr = runtime * 1000000 // gap # floor division, get an integer result
    proc_proc = subprocess.Popen(["./event_proc", event_log_path, policy, str(priority), str(data_ctr), ipc, str(int(pin))])

    time.sleep(runtime)

    # stop the processes first before accessing event_log
    stop_processes()

    # stop all stress_test processes
    if stress:
        for proc in stress_procs:
            proc_signal(proc, signal.SIGINT)
        
        for i, proc in enumerate(stress_procs):
            if not proc_wait(proc, time=2):
                print(f"stress_test{i} did not exit cleanly after SIGINT", file=sys.stderr)
                sys.exit(1)

        print("All stress_test processes exited!")

    # restore orignal setting after tests
    restore_cpu_governors()

    # backup cleanup
    ipc_cleanup()

    # processes and plot the data
    if do_plot is True:
        latency_plot.main(run_index, event_log_path, runtime, gap, policy, priority, ipc, pin, stress)

def pipeline_usage_helper():
    
    print("""
=================================
Arguement Summary
=================================

-r, --runtime     Test runtime in seconds
-g, --gap         Event generation gap in microseconds
-s, --policy      Scheduling policy: other (default), fifo, rr
-p, --priority    Nice value for other, RT priority for fifo/rr (default: 0)
-i, --ipc         IPC mechanism: shm (default) or socket
--pin             Enable CPU pinning (Disable by default)
--stress          Enable stress mode (Disable by default)
--log             Change output log file name
--noplot          Skip latency histogram plotting          

==================================
Pipeline benchmark usage examples
==================================
          
Use socket IPC:
  python3 pipeline_test.py -i socket

Use normal Linux scheduling with nice value -20:
  python3 pipeline_test.py -s other -p -20

Use FIFO real-time scheduling with priority 80:
  sudo python3 pipeline_test.py -s fifo -p 80

Pin event_gen and event_proc threads to specific CPUs:
  sudo python3 pipeline_test.py --pin

Save log to a custom file:
  python3 pipeline_test.py --log custom_log_name.txt

Run benchmark under stress mode:
  python3 pipeline_test.py --stress

Combined example:
  sudo python3 pipeline_test.py -r 10 -g 1000 -s fifo -p 80 -i shm --pin --stress     
    """) # Triple quotes allow strings to span multiple lines without needing the \n escape character


# argparse here makes main() arguements here CLI-sytle strings, like pipeline.main(["-r", "5", "-g", str(gap)])
# so you can't pass commands like pipeline.main(runtime, gap)
def main():
    # creates an argument parser object
    parser = argparse.ArgumentParser()
    # nargs="?" means the argument value is optional: zero or one value
    # type=int convert the argument text into an integer
    # -r is the short form of --runtime, add -- makes it named argument 
    parser.add_argument("-r","--runtime", type=int, default=5) # runtime in s, default 5s
    parser.add_argument("-g", "--gap", type=int, default=1000) # generation gap in us, default 1ms
    parser.add_argument("-s", "--policy", choices=["other", "fifo", "rr"],default="other") # scheduling policy, parameter: other, fifo, rr
    parser.add_argument("-p", "--priority", type=int, default=0) # thread priority (rr, fifo) / nice value (other)
    parser.add_argument("-i", "--ipc", choices=["shm", "socket"], default="shm") # ipc mechanism
    parser.add_argument("--pin", default=False, action="store_true") # pin the all processes to specific CPUs
    parser.add_argument("--stress", default=False, action="store_true") # start the test in stress mode
    parser.add_argument("--log", default="event_log.txt") # output log file name
    parser.add_argument("--noplot",default=True, action="store_false") # add this flag to skip plotting the result histogram
    parser.add_argument("--helper", default=False, action="store_true") # pull out the helper for the program
    # read the command line arguments (sys.argv) and parse them, return the parsed arguments to object named args
    args = parser.parse_args()

    if args.helper:
        pipeline_usage_helper()
        return

    run_pipeline(1, args.runtime, args.gap, args.policy, args.priority, args.ipc, args.pin, args.stress, args.log, args.noplot)


# __name__ is a special variable
# if file is run directly: __name__ == "__main__"
# if file is imported as a module: __name__ is the module name
# run main() only when this file is executed directly
# This prevents main() from running automatically if someone imports pipeline.py
if __name__ == "__main__":
    main()
