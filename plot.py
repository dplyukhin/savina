#!/bin/python
import subprocess
import os
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np

plt.style.use('tableau-colorblind10')
plt.rcParams['figure.figsize'] = [6, 4]

benchmarks = {
    "apsp.ApspAkkaGCActorBenchmark": [100, 200, 300, 400, 500],
    "astar.GuidedSearchAkkaGCActorBenchmark": [10, 20, 30, 40, 50],# -g
    "count.CountingAkkaGCActorBenchmark": [1000000, 2000000, 3000000, 4000000],
    "fib.FibonacciAkkaGCActorBenchmark": [22, 23, 24, 25, 26],
    "nqueenk.NQueensAkkaGCActorBenchmark": [10, 11, 12, 13, 14, 15],
    "quicksort.QuickSortAkkaGCActorBenchmark": [500000, 1000000, 1500000, 2000000, 2500000],
    "radixsort.RadixSortAkkaGCActorBenchmark": [50000, 60000, 70000, 80000, 90000],
    "recmatmul.MatMulAkkaGCActorBenchmark": [1024, 512, 256, 128, 64],
}

def get_stats(filename):
    """
    Read the CSV file and return the average and standard deviation.
    """
    with open(f"raw_data/{filename}") as file:
        lines = [float(line) for line in file]
        return np.average(lines), np.std(lines)


def process_data():
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

        with open(f"processed_data/{benchmark}.csv", "w") as output:
            output.write('"N", "no GC", "no GC error", "WRC", "WRC error", "CRGC (on-block)", "CRGC error (on-block)", "CRGC (wave)", "CRGC error (wave)"\n')
            for param in benchmarks[benchmark]:
                output.write(",".join([str(p) for p in d[param]]) + "\n") 


def plot_ordinary_overhead(benchmark):
    """
    Plot any benchmark (except fibonacci), with overhead in the y-axis.
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
    Plot any benchmark (except fibonacci), with execution time in the y-axis.
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


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Invalid number of arguments. Use 'process' to process the data or 'plot' to plot the data.")
        sys.exit(1)
    if sys.argv[1] == "process":
        process_data()
    elif sys.argv[1] == "plot":
        plot_data()
    else:
        print("Invalid argument. Use 'process' to process the data or 'plot' to plot the data.")
        sys.exit(1)
