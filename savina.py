import argparse
import subprocess
import sys
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np

############################## CONFIGURATION ##############################

# Number of times to run each benchmark.
iter=30

# Which benchmarks to run, and which parameters to run them on.
benchmarks = {
    "apsp.ApspAkkaGCActorBenchmark": [100, 200, 300, 400, 500],
    "astar.GuidedSearchAkkaGCActorBenchmark": [10, 20, 30, 40, 50],
    "count.CountingAkkaGCActorBenchmark": [1000000, 2000000, 3000000, 4000000, 5000000, 6000000],
    "fib.FibonacciAkkaGCActorBenchmark": [22, 23, 24, 25, 26], 
    "nqueenk.NQueensAkkaGCActorBenchmark": [10, 11, 12, 13, 14, 15],
    "quicksort.QuickSortAkkaGCActorBenchmark": [500000, 1000000, 1500000, 2000000, 2500000],
    "radixsort.RadixSortAkkaGCActorBenchmark": [50000, 60000, 70000, 80000, 90000, 100000],
    "recmatmul.MatMulAkkaGCActorBenchmark": [1024, 512, 256, 128, 64],
}

# Pyplot configuration.
plt.style.use('tableau-colorblind10')
plt.rcParams['figure.figsize'] = [6, 4]

# Types of garbage collectors to use
gc_types = ["nogc", "wrc", "mac", "crgc-onblock", "crgc-wave"]

############################## BENCHMARK RUNNER ##############################

opts = {
    "apsp.ApspAkkaGCActorBenchmark": "-n",
    "astar.GuidedSearchAkkaGCActorBenchmark": "-g",
    "count.CountingAkkaGCActorBenchmark": "-n",
    "fib.FibonacciAkkaGCActorBenchmark": "-n",
    "nqueenk.NQueensAkkaGCActorBenchmark": "-n",
    "quicksort.QuickSortAkkaGCActorBenchmark": "-n",
    "radixsort.RadixSortAkkaGCActorBenchmark": "-n",
    "recmatmul.MatMulAkkaGCActorBenchmark": "-n",
}

def raw_time_filename(benchmark, param, gc_type):
    return f"raw_data/{benchmark}-n{param}-{gc_type}.csv"

def raw_times_exist():
    for file in os.listdir('raw_data'):
        if file.endswith('.csv'):
            return True
    return False

def raw_count_filename(benchmark, param, gc_type):
    return f"raw_data/{benchmark}-n{param}-{gc_type}.jfr"

def raw_counts_exist():
    for file in os.listdir('raw_data'):
        if file.endswith('.jfr'):
            return True
    return False

def run_benchmark(benchmark, gc_type, param, options):
    classname = "edu.rice.habanero.benchmarks." + benchmark
    opt = opts[benchmark]

    gc_args = []
    if gc_type == "nogc":
        gc_args = ["-Duigc.engine=manual"]
    elif gc_type == "wrc":
        gc_args = ["-Duigc.engine=mac", "-Duigc.mac.cycle-detection=off"]
    elif gc_type == "mac":
        gc_args = ["-Duigc.engine=mac", "-Duigc.mac.cycle-detection=on"]
    elif gc_type == "crgc-onblock":
        gc_args = ["-Dgc.crgc.collection-style=on-block", "-Duigc.engine=crgc"]
    elif gc_type == "crgc-wave":
        gc_args = ["-Dgc.crgc.collection-style=wave", "-Duigc.engine=crgc"]
    else:
        print(f"Invalid garbage collector type '{gc_type}'. Valid options are: {gc_types.join(', ')}")
        sys.exit(1)

    subprocess.run(["sbt"] + gc_args + [f'runMain {classname} -iter {iter} {options} {opt} {param}'])

def run_time_benchmark(benchmark, gc_type, param):
    filename = raw_time_filename(benchmark, param, gc_type)
    run_benchmark(benchmark, gc_type, param, f"-filename {filename}")

def run_count_benchmark(benchmark, gc_type, param):
    filename = raw_count_filename(benchmark, param, gc_type)
    run_benchmark(benchmark, gc_type, param, f"-jfr-filename {filename}")

def run_time_benchmarks(benchmarks, gc_types, args):
    if raw_times_exist() and not args.append:
        print("There are .csv files in the directory. Either remove them or re-run with the --append flag. Aborting.")
        sys.exit()

    for benchmark in benchmarks:
        for param in benchmarks[benchmark]:
            for gc_type in gc_types:
                run_time_benchmark(benchmark, gc_type, param)

def run_count_benchmarks(benchmarks, gc_types, args):
    if raw_counts_exist():
        print("There are .jfr files in the directory. Please delete them first. Aborting.")
        sys.exit()

    for benchmark in benchmarks:
        for param in benchmarks[benchmark]:
            for gc_type in gc_types:
                run_count_benchmark(benchmark, gc_type, param)


############################## DATA PROCESSING ##############################

def get_time_stats(benchmark, param, gc_type):
    """
    Read the CSV file and return the average and standard deviation.
    """
    filename = raw_time_filename(benchmark, param, gc_type)
    with open(filename) as file:
        lines = [float(line) for line in file]
        return np.average(lines), np.std(lines)

def process_time_data(benchmarks, gc_types, args):
    for benchmark in benchmarks:
        d = {}
        for param in benchmarks[benchmark]:
            d[param] = [param]

            nogc_avg, nogc_std = get_time_stats(benchmark, param, "nogc")
            d[param].append(nogc_avg)
            d[param].append(nogc_std)

            wrc_avg, wrc_std = get_time_stats(benchmark, param, "wrc")
            d[param].append(wrc_avg)
            d[param].append(wrc_std)

            onblk_avg, onblk_std = get_time_stats(benchmark, param, "crgc-block")
            d[param].append(onblk_avg)
            d[param].append(onblk_std)

            wave_avg, wave_std = get_time_stats(benchmark, param, "crgc-wave")
            d[param].append(wave_avg)
            d[param].append(wave_std)

        with open(f"processed_data/{benchmark}.csv", "w") as output:
            output.write('"N", "no GC", "no GC error", "WRC", "WRC error", "CRGC (on-block)", "CRGC error (on-block)", "CRGC (wave)", "CRGC error (wave)"\n')
            for param in benchmarks[benchmark]:
                output.write(",".join([str(p) for p in d[param]]) + "\n") 

def count_messages(benchmark, param, gc_type):
    filename = raw_count_filename(benchmark, param, gc_type)
    subprocess.run(f"jfr print --json {filename} > {filename}.json", shell=True)
    total_app_msgs = 0
    total_ctrl_msgs = 0
    with open(f'{filename}.json', 'r') as f:
        data = json.load(f)
        events = data['recording']['events']
        for event in events:
            if "mac.jfr.ActorBlockedEvent" in event['type']:
                total_app_msgs += event['values']['appMsgCount']
                total_ctrl_msgs += event['values']['ctrlMsgCount']
            elif "mac.jfr.ProcessingMessages" in event['type']:
                total_ctrl_msgs += event['values']['numMessages']
            elif "crgc.jfr.EntryFlushEvent" in event['type']:
                total_app_msgs += event['values']['recvCount']
            elif "crgc.jfr.ProcessingEntries" in event['type']:
                total_ctrl_msgs += event['values']['numEntries']
    with open(f'processed_data/{filename}.csv', 'w') as f:
        f.write(f'{total_app_msgs}, {total_ctrl_msgs}')
        print(f'Wrote processed_data/{filename}.csv')
    os.remove(f"{filename}.json")

def process_count_data(benchmarks, gc_types, args):
    for benchmark in benchmarks:
        for param in benchmarks[benchmark]:
            filename = f"{benchmark}-n{param}-WRC"
            count_messages(filename)
            filename = f"{benchmark}-n{param}-MAC"
            count_messages(filename)
            filename = f"{benchmark}-n{param}-crgc-onblock"
            count_messages(filename)
            filename = f"{benchmark}-n{param}-crgc-wave"
            count_messages(filename)


############################## PLOTTING ##############################

def plot_ordinary_overhead(benchmark):
    """
    Plot a benchmark with overhead in the y-axis.
    """
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(f'processed_data/{benchmark}.csv')

    # Extract the data for x-axis, y-axis, and error bars from the DataFrame
    x_values = df.iloc[:, 0]
    nogc = df.iloc[:, 1]
    nogc_err = df.iloc[:, 2]
    wrc = (df.iloc[:, 3] / nogc * 100) - 100
    crgc_onblk = (df.iloc[:, 5] / nogc * 100) - 100
    crgc_wave = (df.iloc[:, 7] / nogc * 100) - 100

    fig, ax = plt.subplots()

    # Create the plot
    yerr = [abs((nogc - nogc_err) / nogc * 100 - 100), (nogc + nogc_err) / nogc * 100 - 100]
    ax.errorbar(x_values, nogc / nogc * 100 - 100, yerr=yerr, fmt='-o', capsize=5, label="no GC")
    ax.errorbar(x_values, crgc_onblk, fmt='-o', capsize=5, label="CRGC")
    ax.errorbar(x_values, wrc, fmt='-o', capsize=5, label="WRC")
    #ax.errorbar(x_values, crgc_wave,  fmt='-o', capsize=5, label="CRGC (wave)")

    # Add labels and title to the plot
    ax.set_xlabel('N')
    ax.set_ylabel('Execution time overhead (%)')
    #ax.set_title(benchmark)
    #ax.set_ylim(-20)

    # Show the plot
    plt.legend()
    plt.savefig(f'figures/{benchmark}-overhead.pdf', dpi=500)
    print(f"Wrote {benchmark}-overhead.pdf")


def plot_ordinary_time(benchmark):
    """
    Plot any benchmark with execution time in the y-axis.
    """
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(f'processed_data/{benchmark}.csv')

    # Extract the data for x-axis, y-axis, and error bars from the DataFrame
    x_values = df.iloc[:, 0]
    nogc = df.iloc[:, 1]
    nogc_err = df.iloc[:, 2]
    wrc = df.iloc[:, 3]
    wrc_err = df.iloc[:, 4]
    crgc_onblk = df.iloc[:, 5]
    crgc_onblk_err = df.iloc[:, 6]
    crgc_wave = df.iloc[:, 7]
    crgc_wave_err = df.iloc[:, 8]

    fig, ax = plt.subplots()

    # Create the plot
    #ax.set_yscale('log', base=10)
    #ax.grid()
    ax.errorbar(x_values, nogc, yerr=nogc_err, fmt='-o', capsize=5, label="no GC")
    ax.errorbar(x_values, crgc_onblk, yerr=crgc_onblk_err, fmt='-o', capsize=5, label="CRGC")
    ax.errorbar(x_values, wrc, yerr=wrc_err, fmt='-o', capsize=5, label="WRC")
    #ax.errorbar(x_values, crgc_wave,  yerr=crgc_wave_err,  fmt='-o', capsize=5, label="CRGC (wave)")

    # Add labels and title to the plot
    ax.set_xlabel('N')
    ax.set_ylabel('Execution time (ms)')
    #ax.set_title(benchmark)
    ax.set_ylim(bottom=1)

    # Show the plot
    plt.legend()
    plt.savefig(f'figures/{benchmark}-time.pdf', dpi=500)
    print(f"Wrote {benchmark}-time.pdf")
    #plt.show()

def plot_data():
    for bm in benchmarks.keys():
        plot_ordinary_overhead(bm)
        plot_ordinary_time(bm)


############################## MAIN ##############################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Savina benchmarks and plot results.'
    )
    parser.add_argument(
        "command", 
        choices=["simple_eval", "full_eval", "run", "process", "plot"], 
        help="What command to run."
    )
    parser.add_argument(
        "--append", 
        action="store_true", 
        help="Append benchmark times to raw data, instead of overwriting them."
    )
    args = parser.parse_args()

    # Create raw data, processed data, and figures directories if they don't already exist.
    os.makedirs('raw_data', exist_ok=True)
    os.makedirs('processed_data', exist_ok=True)
    os.makedirs('figures', exist_ok=True)

    if args.command == "simple_eval":
        run_time_benchmarks(benchmarks, gc_types, args)
        process_time_data()
        plot_data()
    elif args.command == "full_eval":
        run_time_benchmarks(benchmarks, gc_types, args)
        run_count_benchmarks(benchmarks, gc_types, args)
        process_time_data()
        process_count_data()
        plot_data()
    elif args.command == "process":
        process_time_data()
        process_count_data()
    elif args.command == "plot":
        plot_data()
    else:
        parser.print_help()

