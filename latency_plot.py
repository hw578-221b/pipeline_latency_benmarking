import numpy as np
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from pathlib import Path # pathlib is the module, Path is a class inside that module
# import pathlib as Path, this imports the entire pathlib module, but gives the module the nickname Path
# use output_dir = Path.Path("latency_plots") if use above code

def latency_statistics(latency):
    sorted_latency = np.sort(latency)
    lat_min = sorted_latency[0]
    lat_max = sorted_latency[-1]
    lat_avg = np.mean(sorted_latency)
    lat_p99 = np.percentile(sorted_latency, 99)

    return lat_min, lat_max, lat_avg, lat_p99

def make_hist_line(latency, bins=200):
    # np.histogram(...) groups your raw latency data into bins
    # bins=200 means: split the data range into 200 intervals
    counts, bin_edges = np.histogram(latency, bins=bins)
    # python string slicing operations
    # s[start:end] slices the string from index start to end-1, s[start:] slices from start to the end of the string, s[:end] slices from the beginning to end-1, s[-n:] slices the last n characters, s[:-n] slices from the beginning to the character at index -n-1
    # s = "abcdefghijklmno"
    # s[-4:] slices the string starting from the 4th character from the end ('m') to the end of the string.
    # s[:-3] slices the string from the beginning up to the 3rd character from the end ('k'), excluding it.
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    # add a zero value at the beginning of latency to visualize the sudden jump up of the curve
    # inserts one extra value (second 0) into the beginning (first 0) of counts
    y = np.insert(counts, 0, 0)
    x = np.insert(bin_centers, 0, bin_edges[0])

    return x, y

def hist_style(title):
    plt.yscale("log")
    plt.xlabel("Latency (us)")
    plt.ylabel("Occurrences")
    plt.title(title)
    plt.grid(True, which="both")
    plt.legend()  # Automatically picks up the labels above
    plt.tight_layout()


def main(run_index=1, log_file="event_log.txt", runtime=None, gap=None, policy="other", priority=None, ipc="shm", pin=False, stress=False):

    print("Statistical calculation and data plotting started...")
    
    data = np.loadtxt(log_file)

    # values in python list can be literal strings, numbers, variables, function results, expressions
    tags = [
        f"r{runtime}",
        f"g{gap}",
        policy,
        f"p{priority}",
        ipc,
        "pinned" if pin else "unpinned", # conditional expression
        "stress" if stress else "nostress"
    ]

    # join() combines a list of strings into one string
    # The string before .join() is the separator
    tag_str = "_".join(tags)

    # creates a path object in python representing the folder
    # then create a statistics file path based on that
    if runtime is not None and gap is not None and priority is not None:
        output_dir = Path(f"Test{run_index}_{tag_str}")
        stats_file = output_dir / f"latency_statistics_{tag_str}.txt"
    else:
        output_dir = Path(f"Test{run_index}_plots")
        stats_file = output_dir / f"latency_statistics.txt"

    # creates the folder if it doesn't exist, if exists, exist_ok=True prevents FileExistsError.
    output_dir.mkdir(exist_ok=True)

    latency0 = data[:, 0] / 1000
    latency1 = data[:, 1] / 1000
    latency2 = data[:, 2] / 1000
    latency3 = data[:, 3] / 1000

    latencies = [
        (latency0, "latency0", "latency0 Histogram (from event generation to capture)", "latency0_histogram.png"),
        (latency1, "latency1", "Latency1 Histogram (from event capture to process)", "latency1_histogram.png"),
        (latency2, "latency2", "Latency2 Histogram (from event process to respond)", "latency2_histogram.png"),
        (latency3, "latency3", "Latency3 Histogram (from scheduled and actual generation time)", "latency3_histogram.png")
    ]

    # Calculate and print the latency statistics, then plot 4 latencies separately
    # enumerate adds a counter to an iterable (like a list, tuple, or string) 
    for i, (latency, label, title, filename) in enumerate(latencies):

        lat_min, lat_max, lat_avg, lat_p99 = latency_statistics(latency)
        info = f"Latency {i} -- Min: {lat_min:.2f} us, Max: {lat_max:.2f} us, Avg: {lat_avg:.2f} us, p99: {lat_p99:.2f} us"

        # print to both the terminal and the file
        with open(stats_file, "a") as f:
            print(info, file=f)
            print(info)

        x, y = make_hist_line(latency)
        plt.figure() # create a new "Figure" object, acts as the top-level container for all plot elements (axes, titles, and labels)
        plt.plot(x, y, linewidth=2, label=label)
        hist_style(title)
        plt.savefig(output_dir / filename, dpi=200)
        plt.close()  # close a figure window and release the associated memory

    # Plot the all event passing latencies (except gen_delay) in one combined figure
    plt.figure()
    for latency, label, title, filename in latencies[:3]:
        x, y = make_hist_line(latency)
        
        plt.plot(x, y, linewidth=2, label=label)

    hist_style("Combined Histogram")
    plt.savefig(output_dir / "combined_histogram.png", dpi=200)
    plt.close()


if __name__ == "__main__":
    main()