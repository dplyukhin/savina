#!/usr/bin/env python3

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

# Which benchmarks to run, and which parameters to run them on.
benchmarks = {
    "apsp.ApspAkkaGCActorBenchmark": [500],   # [100, 200, 300, 400, 500],
    "astar.GuidedSearchAkkaGCActorBenchmark": [50],    # [10, 20, 30, 40, 50],
    "banking.BankingAkkaManualStashActorBenchmark": [50_000],
    #"barber.SleepingBarberAkkaActorBenchmark": [5_000],         # Skipped due to unstable results
    "big.BigAkkaActorBenchmark": [2_000],
    "bitonicsort.BitonicSortAkkaActorBenchmark": [4096],
    "bndbuffer.ProdConsAkkaActorBenchmark": [1000],
    "chameneos.ChameneosAkkaActorBenchmark": [400_000],
    "cigsmok.CigaretteSmokerAkkaActorBenchmark": [1000],
    "concdict.DictionaryAkkaActorBenchmark": [10_000],
    "concsll.SortedListAkkaActorBenchmark": [8000],
    "count.CountingAkkaGCActorBenchmark": [3_000_000],   # [1000000, 2000000, 3000000, 4000000, 5000000],
    "facloc.FacilityLocationAkkaActorBenchmark": [100_000],
    "fib.FibonacciAkkaGCActorBenchmark": [25],     # [22, 23, 24, 25, 26],
    #"filterbank.FilterBankAkkaActorBenchmark": [34816],         # Skipped due to deadlocks
    "fjcreate.ForkJoinAkkaActorBenchmark": [200_000],
    "fjthrput.ThroughputAkkaActorBenchmark": [50_000],
    "logmap.LogisticMapAkkaManualStashActorBenchmark": [25_000],
    "nqueenk.NQueensAkkaGCActorBenchmark": [13],    # [9, 10, 11, 12, 13],
    "philosopher.PhilosopherAkkaActorBenchmark": [20],
    "pingpong.PingPongAkkaActorBenchmark": [500_000],
    "piprecision.PiPrecisionAkkaActorBenchmark": [5_000],
    "quicksort.QuickSortAkkaGCActorBenchmark": [2_000_000],     # [500000, 1000000, 1500000, 2000000, 2500000],
    "radixsort.RadixSortAkkaGCActorBenchmark": [100_000],       # [50000, 60000, 70000, 80000, 90000],
    "recmatmul.MatMulAkkaGCActorBenchmark": [1024],    # [1024, 512, 256, 128, 64],
    "sieve.SieveAkkaActorBenchmark": [100_000],
    #"sor.SucOverRelaxAkkaActorBenchmark": [0],        # Skipped due to deadlock
    "threadring.ThreadRingAkkaActorBenchmark": [100],
    "trapezoid.TrapezoidalAkkaActorBenchmark": [10_000_000],
    "uct.UctAkkaActorBenchmark": [200_000],
}

# Which benchmarks to skip in the simple evaluation.
skippable_benchmarks = [
]

# Pyplot configuration.
plt.style.use('tableau-colorblind10')
plt.rcParams['figure.figsize'] = [6, 4]

# Types of garbage collectors to use
gc_types = ["nogc", "wrc", "crgc-onblock", "crgc-wave"]

############################## BENCHMARK RUNNER ##############################

opts = {
    "apsp.ApspAkkaGCActorBenchmark": "-n",
    "astar.GuidedSearchAkkaGCActorBenchmark": "-g",
    "banking.BankingAkkaManualStashActorBenchmark": "-n",
    #"barber.SleepingBarberAkkaActorBenchmark": "-n",
    "big.BigAkkaActorBenchmark": "-n",
    "bitonicsort.BitonicSortAkkaActorBenchmark": "-n",
    "bndbuffer.ProdConsAkkaActorBenchmark": "-ipp",
    "chameneos.ChameneosAkkaActorBenchmark": "-m",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark": "-r",
    "concdict.DictionaryAkkaActorBenchmark": "-m",
    "concsll.SortedListAkkaActorBenchmark": "-m",
    "count.CountingAkkaGCActorBenchmark": "-n",
    "facloc.FacilityLocationAkkaActorBenchmark": "-n",
    "fib.FibonacciAkkaGCActorBenchmark": "-n",
    "filterbank.FilterBankAkkaActorBenchmark": "-sim",
    "fjcreate.ForkJoinAkkaActorBenchmark": "-n",
    "fjthrput.ThroughputAkkaActorBenchmark": "-n",
    "logmap.LogisticMapAkkaManualStashActorBenchmark": "-t",
    "nqueenk.NQueensAkkaGCActorBenchmark": "-n",
    "philosopher.PhilosopherAkkaActorBenchmark": "-n",
    "pingpong.PingPongAkkaActorBenchmark": "-n",
    "piprecision.PiPrecisionAkkaActorBenchmark": "-p",
    "quicksort.QuickSortAkkaGCActorBenchmark": "-n",
    "radixsort.RadixSortAkkaGCActorBenchmark": "-n",
    "recmatmul.MatMulAkkaGCActorBenchmark": "-n",
    "sieve.SieveAkkaActorBenchmark": "-n",
    "sor.SucOverRelaxAkkaActorBenchmark": "-n",
    "threadring.ThreadRingAkkaActorBenchmark": "-n",
    "trapezoid.TrapezoidalAkkaActorBenchmark": "-n",
    "uct.UctAkkaActorBenchmark": "-nodes"
}
benchmarkName = {
    "apsp.ApspAkkaGCActorBenchmark": "All-Pairs Shortest Path",
    "astar.GuidedSearchAkkaGCActorBenchmark": "A-Star Search",
    "banking.BankingAkkaManualStashActorBenchmark": "Bank Transaction",
    #"barber.SleepingBarberAkkaActorBenchmark": "Sleeping Barber",
    "big.BigAkkaActorBenchmark": "Big",
    "bitonicsort.BitonicSortAkkaActorBenchmark": "Bitonic Sort",
    "bndbuffer.ProdConsAkkaActorBenchmark": "Producer-Consumer",
    "chameneos.ChameneosAkkaActorBenchmark": "Chameneos",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark": "Cigarette Smokers",
    "concdict.DictionaryAkkaActorBenchmark": "Concurrent Dictionary",
    "concsll.SortedListAkkaActorBenchmark": "Concurrent Sorted Linked List",
    "count.CountingAkkaGCActorBenchmark": "Counting Actor",
    "facloc.FacilityLocationAkkaActorBenchmark": "Online Facility Location",
    "fib.FibonacciAkkaGCActorBenchmark": "Recursive Fibonacci Tree",
    "filterbank.FilterBankAkkaActorBenchmark": "Filter Bank",
    "fjcreate.ForkJoinAkkaActorBenchmark": "Fork Join (Actor Creation)",
    "fjthrput.ThroughputAkkaActorBenchmark": "Fork Join (Throughput)",
    "logmap.LogisticMapAkkaManualStashActorBenchmark": "Logistic Map Series",
    "nqueenk.NQueensAkkaGCActorBenchmark": "N-Queens",
    "philosopher.PhilosopherAkkaActorBenchmark": "Dining Philosophers",
    "pingpong.PingPongAkkaActorBenchmark": "Ping Pong",
    "piprecision.PiPrecisionAkkaActorBenchmark": "Precise Pi Calculation",
    "quicksort.QuickSortAkkaGCActorBenchmark": "Recursive Tree Quicksort",
    "radixsort.RadixSortAkkaGCActorBenchmark": "Radix Sort",
    "recmatmul.MatMulAkkaGCActorBenchmark": "Recursive Matrix Multiplication",
    "sieve.SieveAkkaActorBenchmark": "Sieve of Eratosthenes",
    "sor.SucOverRelaxAkkaActorBenchmark": "Successive Over-Relaxation",
    "threadring.ThreadRingAkkaActorBenchmark": "Thread Ring",
    "trapezoid.TrapezoidalAkkaActorBenchmark": "Trapezoidal Approximation",
    "uct.UctAkkaActorBenchmark": "Unbalanced Cobwebbed Tree",
}
microBenchmarks = [
    "big.BigAkkaActorBenchmark",
    "chameneos.ChameneosAkkaActorBenchmark",
    "count.CountingAkkaGCActorBenchmark",
    "fib.FibonacciAkkaGCActorBenchmark",
    "fjcreate.ForkJoinAkkaActorBenchmark",
    "fjthrput.ThroughputAkkaActorBenchmark",
    "pingpong.PingPongAkkaActorBenchmark",
]
concurrentBenchmarks = [
    "banking.BankingAkkaManualStashActorBenchmark",
    #"barber.SleepingBarberAkkaActorBenchmark",
    "bndbuffer.ProdConsAkkaActorBenchmark",
    "cigsmok.CigaretteSmokerAkkaActorBenchmark",
    "concdict.DictionaryAkkaActorBenchmark",
    "concsll.SortedListAkkaActorBenchmark",
    "logmap.LogisticMapAkkaManualStashActorBenchmark",
    "philosopher.PhilosopherAkkaActorBenchmark",
]
parallelBenchmarks = [
    "apsp.ApspAkkaGCActorBenchmark",
    "astar.GuidedSearchAkkaGCActorBenchmark",
    "bitonicsort.BitonicSortAkkaActorBenchmark",
    "facloc.FacilityLocationAkkaActorBenchmark",
    "nqueenk.NQueensAkkaGCActorBenchmark",
    "piprecision.PiPrecisionAkkaActorBenchmark",
    "quicksort.QuickSortAkkaGCActorBenchmark",
    "radixsort.RadixSortAkkaGCActorBenchmark",
    "recmatmul.MatMulAkkaGCActorBenchmark",
    "sieve.SieveAkkaActorBenchmark",
    "trapezoid.TrapezoidalAkkaActorBenchmark",
    "uct.UctAkkaActorBenchmark",
]


def raw_time_filename(benchmark, param, gc_type):
    return f"raw_data/{benchmark}-{param}-{gc_type}.csv"

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

def run_benchmark(benchmark, gc_type, param, options, args):
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
        gc_args = ["-Duigc.crgc.collection-style=on-block", "-Duigc.engine=crgc"]
    elif gc_type == "crgc-wave":
        gc_args = ["-Duigc.crgc.collection-style=wave", "-Duigc.engine=crgc"]
    else:
        print(f"Invalid garbage collector type '{gc_type}'. Valid options are: {gc_types.join(', ')}")
        sys.exit(1)

    subprocess.run(["sbt", "-J-Xmx16G", "-J-XX:+UseZGC"] + gc_args + [f'runMain {classname} -iter {args.iter} {options} {opt} {param}'])

def run_time_benchmark(benchmark, gc_type, param, args):
    filename = raw_time_filename(benchmark, param, gc_type)
    run_benchmark(benchmark, gc_type, param, f"-filename {filename}", args)

def run_count_benchmark(benchmark, gc_type, param, args):
    filename = raw_count_filename(benchmark, param, gc_type)
    run_benchmark(benchmark, gc_type, param, f"-jfr-filename {filename}", args)


############################## DATA PROCESSING ##############################

def processed_time_filename(benchmark):
    return f"processed_data/{benchmark}.csv"

def processed_count_filename(benchmark, param, gc_type):
    return f"processed_data/{benchmark}-{param}-{gc_type}-count.csv"

def get_time_stats(benchmark, param, gc_type):
    """
    Read the CSV file and return the average and standard deviation.
    """
    filename = raw_time_filename(benchmark, param, gc_type)
    with open(filename) as file:
        lines = [float(line) for line in file]
        # Only keep the 40% lowest values
        lines = sorted(lines)[:int(len(lines) * 0.4)]
        return np.average(lines), np.std(lines)

def process_time_data(benchmark, params):
    d = {}
    for param in params:
        d[param] = [param]

        nogc_avg, nogc_std = get_time_stats(benchmark, param, "nogc")
        d[param].append(nogc_avg)
        d[param].append(nogc_std)

        wrc_avg, wrc_std = get_time_stats(benchmark, param, "wrc")
        d[param].append(wrc_avg)
        d[param].append(wrc_std)

        onblk_avg, onblk_std = get_time_stats(benchmark, param, "crgc-onblock")
        d[param].append(onblk_avg)
        d[param].append(onblk_std)

        wave_avg, wave_std = get_time_stats(benchmark, param, "crgc-wave")
        d[param].append(wave_avg)
        d[param].append(wave_std)

    filename = processed_time_filename(benchmark)
    with open(filename, "w") as output:
        output.write('"N", "no GC", "no GC error", "WRC", "WRC error", "CRGC (on-block)", "CRGC error (on-block)", "CRGC (wave)", "CRGC error (wave)"\n')
        for param in params:
            output.write(",".join([str(p) for p in d[param]]) + "\n")

def shorten_benchmark_name(benchmark):
    return benchmark.split(".")[0]

def sigfigs(x, n):
    """
    Round x to n significant figures.
    """
    y = round(x, n - int(np.floor(np.log10(abs(x)))) - 1)
    return int(y) if y.is_integer() else y

def cellcolor(stdev, overhead):
    if overhead < 0:
        return f"\\cellcolor{{green!{abs(overhead) / 200 * 60}}}{overhead}"
    else:
        return f"\\cellcolor{{red!{overhead / 200 * 60}}}{overhead}"

def process_all_times(benchmarkList):
    d = {}
    for bm in benchmarkList:
        d[bm] = {}
        for param in benchmarks[bm]:
            d[bm][param] = ["\\texttt{" + shorten_benchmark_name(bm) + "}"]

            nogc_avg, nogc_std = get_time_stats(bm, param, "nogc")
            d[bm][param].append(sigfigs(nogc_avg / 1000, 2))

            wrc_avg, _ = get_time_stats(bm, param, "wrc")
            d[bm][param].append(sigfigs(wrc_avg / 1000, 2))

            onblk_avg, onblk_std = get_time_stats(bm, param, "crgc-onblock")
            d[bm][param].append(sigfigs(onblk_avg / 1000, 2))

            wave_avg, wave_std = get_time_stats(bm, param, "crgc-wave")
            d[bm][param].append(sigfigs(wave_avg / 1000, 2))

            percent_stdev = int(nogc_std / nogc_avg * 100)
            d[bm][param].append("±" + str(percent_stdev))
            d[bm][param].append(cellcolor(percent_stdev, int((wrc_avg / nogc_avg - 1) * 100)))
            d[bm][param].append(cellcolor(percent_stdev, int((onblk_avg / nogc_avg - 1) * 100)))
            d[bm][param].append(cellcolor(percent_stdev, int((wave_avg / nogc_avg - 1) * 100)))

    with open("processed_data/savina.tex", "w") as output:
        output.write('Benchmark, no GC, WRC, CRGC-block, CRGC-wave, no GC (stdev), WRC, CRGC-block, CRGC-wave"\n')
        for bm in microBenchmarks:
            for param in benchmarks[bm]:
                output.write(" & ".join([str(p) for p in d[bm][param]]) + "\\\\\n")
        for bm in concurrentBenchmarks:
            for param in benchmarks[bm]:
                output.write(" & ".join([str(p) for p in d[bm][param]]) + "\\\\\n")
        for bm in parallelBenchmarks:
            for param in benchmarks[bm]:
                output.write(" & ".join([str(p) for p in d[bm][param]]) + "\\\\\n")

############################## PLOTTING ##############################

def plot_ordinary_overhead(benchmark):
    """
    Plot a benchmark with overhead in the y-axis.
    """
    # Read the CSV file into a pandas DataFrame
    filename = processed_time_filename(benchmark)
    df = pd.read_csv(filename)

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
    ax.errorbar(x_values, crgc_wave,  fmt='-o', capsize=5, label="CRGC (wave)")

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
    Plot a benchmark with execution time in the y-axis.
    """
    # Read the CSV file into a pandas DataFrame
    filename = processed_time_filename(benchmark)
    df = pd.read_csv(filename)

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
    ax.errorbar(x_values, crgc_wave,  yerr=crgc_wave_err,  fmt='-o', capsize=5, label="CRGC (wave)")

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

############################## RUNNER ##############################

class BenchmarkRunner:

    def __init__(self, benchmarks, gc_types, args):
        self.benchmarks = benchmarks
        self.gc_types = gc_types
        self.args = args

    def run_time_benchmarks(self):
        if raw_times_exist() and not self.args.append:
            print("There are .csv files in the directory. Either remove them or re-run with the --append flag. Aborting.")
            sys.exit()

        for i in range(self.args.invocations):
            for benchmark in self.benchmarks:
                for param in benchmarks[benchmark]:
                    for gc_type in self.gc_types:
                        run_time_benchmark(benchmark, gc_type, param, self.args)

    def run_count_benchmarks(self):
        if raw_counts_exist():
            print("There are .jfr files in the directory. Please delete them first. Aborting.")
            sys.exit()

        for benchmark in self.benchmarks:
            for param in benchmarks[benchmark]:
                for gc_type in self.gc_types:
                    run_count_benchmark(benchmark, gc_type, param, self.args)

    def process_time_data(self):
        for bm in self.benchmarks:
            params = benchmarks[bm]
            process_time_data(bm, params)
        process_all_times(self.benchmarks)

    def plot_time_data(self):
        for bm in self.benchmarks:
            plot_ordinary_overhead(bm)
            plot_ordinary_time(bm)


############################## MAIN ##############################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run Savina benchmarks and plot results.'
    )
    parser.add_argument(
        "command",
        choices=["simple_eval", "full_eval", "process", "plot"],
        help="What command to run."
    )
    parser.add_argument(
        "--append", 
        action="store_true", 
        help="Append benchmark times to raw data, instead of overwriting them."
    )
    parser.add_argument(
        "--iter",
        type=int,
        default=20,
        help="Number of times to run each benchmark PER JVM INVOCATION."
    )
    parser.add_argument(
        "--invocations",
        type=int,
        default=6,
        help="Number of JVM invocations to run for each benchmark."
    )
    args = parser.parse_args()

    # Create raw data, processed data, and figures directories if they don't already exist.
    os.makedirs('raw_data', exist_ok=True)
    os.makedirs('processed_data', exist_ok=True)
    os.makedirs('figures', exist_ok=True)

    if args.command == "simple_eval":
        bms = [bm for bm in benchmarks if bm not in skippable_benchmarks]
        runner = BenchmarkRunner(bms, gc_types, args)
        runner.run_time_benchmarks()
        runner.process_time_data()
        runner.plot_time_data()
    elif args.command == "full_eval":
        bms = benchmarks.keys()
        runner = BenchmarkRunner(bms, gc_types, args)
        runner.run_time_benchmarks()
        runner.process_time_data()
        runner.plot_time_data()
    elif args.command == "process":
        bms = benchmarks.keys()
        runner = BenchmarkRunner(bms, gc_types, args)
        runner.process_time_data()
    elif args.command == "plot":
        bms = benchmarks.keys()
        runner = BenchmarkRunner(bms, gc_types, args)
        runner.process_time_data()
        runner.plot_time_data()
    else:
        parser.print_help()

