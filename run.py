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

# Where to write raw data.
raw_data_dir = "raw_data"
processed_data_dir = "processed_data"
figures_dir = "figures"

# Pyplot configuration.
plt.style.use('tableau-colorblind10')
plt.rcParams['figure.figsize'] = [6, 4]


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

def run_time_benchmarks():
    """
    Run the benchmarks, measuring execution time and writing
    CSV files to the raw data directory.
    """
    # Create the raw data directory if it doesn't exist
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    # Run the benchmarks
    for benchmark in benchmarks.keys():
        opt = opts[benchmark]
        for param in benchmarks[benchmark]:
            classname = "edu.rice.habanero.benchmarks." + benchmark

            # No GC
            filename = f"{benchmark}-n{param}-nogc"
            subprocess.run(
                ["sbt",
                f"-Duigc.engine=manual",
                f'runMain {classname} -iter {iter} -filename {raw_data_dir}/{filename}.csv {opt} {param}'])

            # WRC
            filename = f"{benchmark}-n{param}-WRC"
            subprocess.run(
                ["sbt",
                f"-Duigc.engine=mac", "-Duigc.mac.cycle-detection=off",
                f'runMain {classname} -iter {iter} -filename {raw_data_dir}/{filename}.csv {opt} {param}'])

            # CRGC on-block
            filename = f"{benchmark}-n{param}-crgc-onblock"
            subprocess.run(
                ["sbt",
                f"-Dgc.crgc.collection-style=on-block", f"-Duigc.engine=crgc",
                f'runMain {classname} -iter {iter} -filename {raw_data_dir}/{filename}.csv {opt} {param}'])

            # CRGC wave
            filename = f"{benchmark}-n{param}-crgc-wave"
            subprocess.run(
                ["sbt",
                f"-Dgc.crgc.collection-style=wave", f"-Duigc.engine=crgc",
                f'runMain {classname} -iter {iter} -filename {raw_data_dir}/{filename}.csv {opt} {param}'])


def run_count_benchmarks():
    """
    Run the benchmarks, measuring message counts and writing
    JFR files to the raw data directory.
    """
    # Create the raw data directory if it doesn't exist
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    for benchmark in benchmarks.keys():
        opt = opts[benchmark]
        for param in benchmarks[benchmark]:
            classname = "edu.rice.habanero.benchmarks." + benchmark

            # WRC
            filename = f"{benchmark}-n{param}-WRC"
            subprocess.run(
                ["sbt",
                f"-Duigc.engine=mac", "-Duigc.mac.cycle-detection=off",
                f'runMain {classname} -iter {iter} -jfr-filename {raw_data_dir}/{filename}.jfr {opt} {param}'])

            # MAC
            filename = f"{benchmark}-n{param}-MAC"
            subprocess.run(
                ["sbt",
                f"-Duigc.engine=mac", "-Duigc.mac.cycle-detection=on",
                f'runMain {classname} -iter {iter} -jfr-filename {raw_data_dir}/{filename}.jfr {opt} {param}'])

            # CRGC on-block
            filename = f"{benchmark}-n{param}-crgc-onblock"
            subprocess.run(
                ["sbt",
                f"-Dgc.crgc.collection-style=on-block", f"-Duigc.engine=crgc",
                f'runMain {classname} -iter {iter} -jfr-filename  {raw_data_dir}/{filename}.jfr {opt} {param}'])

            # CRGC wave
            filename = f"{benchmark}-n{param}-crgc-wave"
            subprocess.run(
                ["sbt",
                f"-Dgc.crgc.collection-style=wave", f"-Duigc.engine=crgc",
                f'runMain {classname} -iter {iter} -jfr-filename {raw_data_dir}/{filename}.jfr {opt} {param}'])

############################## DATA PROCESSING ##############################

def get_stats(filename):
    """
    Read the CSV file and return the average and standard deviation.
    """
    with open(f"{raw_data_dir}/{filename}") as file:
        lines = [float(line) for line in file]
        return np.average(lines), np.std(lines)

def process_time_data():
    """
    Process the raw data into a format that can be plotted.
    """
    # Create the processed data directory if it doesn't exist
    if not os.path.exists(processed_data_dir):
        os.makedirs(processed_data_dir)

    for benchmark in benchmarks.keys():
        d = {}
        for param in benchmarks[benchmark]:
            d[param] = [param]

            nogc_avg, nogc_std = get_stats(f"{benchmark}-n{param}-nogc.csv")
            d[param].append(nogc_avg)
            d[param].append(nogc_std)

            wrc_avg, wrc_std = get_stats(f"{benchmark}-n{param}-WRC.csv")
            d[param].append(wrc_avg)
            d[param].append(wrc_std)

            onblk_avg, onblk_std = get_stats(f"{benchmark}-n{param}-crgc-onblock.csv")
            d[param].append(onblk_avg)
            d[param].append(onblk_std)

            wave_avg, wave_std = get_stats(f"{benchmark}-n{param}-crgc-wave.csv")
            d[param].append(wave_avg)
            d[param].append(wave_std)

        with open(f"{processed_data_dir}/{benchmark}.csv", "w") as output:
            output.write('"N", "no GC", "no GC error", "WRC", "WRC error", "CRGC (on-block)", "CRGC error (on-block)", "CRGC (wave)", "CRGC error (wave)"\n')
            for param in benchmarks[benchmark]:
                output.write(",".join([str(p) for p in d[param]]) + "\n") 

def count_messages(filename):
    subprocess.run(f"jfr print --json {raw_data_dir}/{filename}.jfr > {raw_data_dir}/{filename}.json", shell=True)
    total_app_msgs = 0
    total_ctrl_msgs = 0
    with open(f'{raw_data_dir}/{filename}.json', 'r') as f:
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
    with open(f'{processed_data_dir}/{filename}.csv', 'w') as f:
        f.write(f'{total_app_msgs}, {total_ctrl_msgs}')
        print(f'Wrote {processed_data_dir}/{filename}.csv')
    os.remove(f"{raw_data_dir}/{filename}.json")

def process_count_data():
    # Process the data
    for benchmark in benchmarks.keys():
        opt = opts[benchmark]
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
    df = pd.read_csv(f'{processed_data_dir}/{benchmark}.csv')

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
    plt.savefig(f'{figures_dir}/{benchmark}-overhead.pdf', dpi=500)
    print(f"Wrote {benchmark}-overhead.pdf")


def plot_ordinary_time(benchmark):
    """
    Plot any benchmark with execution time in the y-axis.
    """
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(f'{processed_data_dir}/{benchmark}.csv')

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
    plt.savefig(f'{figures_dir}/{benchmark}-time.pdf', dpi=500)
    print(f"Wrote {benchmark}-time.pdf")
    #plt.show()

def plot_data():
    # Create the figres directory if it doesn't exist
    if not os.path.exists(figures_dir):
        os.makedirs(figures_dir)

    for bm in benchmarks.keys():
        plot_ordinary_overhead(bm)
        plot_ordinary_time(bm)


############################## MAIN ##############################

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Invalid argument. Argument may be 'runtime', 'count', 'process', or 'plot'.")
        sys.exit(1)
    if sys.argv[1] == "runtime":
        # Check if there are any .csv files in the directory. If the "--append" flag is not used, abort.
        for file in os.listdir(raw_data_dir):
            if file.endswith('.csv') and "--append" not in sys.argv:
                print("There are .csv files in the directory. Either remove them or re-run with the --append flag. Aborting.")
                sys.exit()
        run_time_benchmarks()
    elif sys.argv[1] == "count":
        # Check if there are any .jfr files in the directory. If so, abort.
        for file in os.listdir('.'):
            if file.endswith('.jfr') or file.endswith('.json') or file.endswith('.csv'):
                print("There are .jfr, .json, or .csv files in the directory. Please delete or move them first. Aborting.")
                sys.exit()
        run_count_benchmarks()
    elif sys.argv[1] == "process":
        process_time_data()
    elif sys.argv[1] == "plot":
        plot_data()
    else:
        print("Invalid argument. Argument may be 'runtime', 'count', 'process', or 'plot'.")
        sys.exit(1)
