#!/bin/bash
import subprocess
import os
import sys

"""
This script runs the benchmarks to compute message counts.
"""

benchmarks = {
    "apsp.ApspAkkaGCActorBenchmark": [100, 200, 300, 400, 500],
    "astar.GuidedSearchAkkaGCActorBenchmark": [10, 20, 30, 40, 50],# -g
    #"big.BigAkkaGCActorBenchmark", 
    #"chameneos.ChameneosAkkaGCActorBenchmark", 
    "count.CountingAkkaGCActorBenchmark": [1000000, 2000000, 3000000, 4000000, 5000000, 6000000],
    "fib.FibonacciAkkaGCActorBenchmark": [22, 23, 24, 25, 26], 
    #"fjcreate.ForkJoinAkkaGCActorBenchmark", 
    #"fjthrput.ThroughputAkkaGCActorBenchmark", 
    "nqueenk.NQueensAkkaGCActorBenchmark": [10, 11, 12, 13, 14, 15],
    #"pingpong.PingPongAkkaGCActorBenchmark", 
    "quicksort.QuickSortAkkaGCActorBenchmark": [500000, 1000000, 1500000, 2000000, 2500000],
    "radixsort.RadixSortAkkaGCActorBenchmark": [50000, 60000, 70000, 80000, 90000, 100000],
    "recmatmul.MatMulAkkaGCActorBenchmark": [1024, 512, 256, 128, 64],
    #"threadring.ThreadRingAkkaGCActorBenchmark"
}

opts = {
    "apsp.ApspAkkaGCActorBenchmark": "-n",
    "astar.GuidedSearchAkkaGCActorBenchmark": "-g",
    #"big.BigAkkaGCActorBenchmark", 
    #"chameneos.ChameneosAkkaGCActorBenchmark", 
    "count.CountingAkkaGCActorBenchmark": "-n",
    "fib.FibonacciAkkaGCActorBenchmark": "-n",
    #"fjcreate.ForkJoinAkkaGCActorBenchmark", 
    #"fjthrput.ThroughputAkkaGCActorBenchmark", 
    "nqueenk.NQueensAkkaGCActorBenchmark": "-n",
    #"pingpong.PingPongAkkaGCActorBenchmark", 
    "quicksort.QuickSortAkkaGCActorBenchmark": "-n",
    "radixsort.RadixSortAkkaGCActorBenchmark": "-n",
    "recmatmul.MatMulAkkaGCActorBenchmark": "-n",
    #"threadring.ThreadRingAkkaGCActorBenchmark"
}

iter=10

# Check if there are any .jfr files in the directory. If so, abort.
for file in os.listdir('.'):
    if file.endswith('.jfr') or file.endswith('.json'):
        print("There are .jfr or .json files in the directory. Please delete or move them first. Aborting.")
        sys.exit()

# Run benchmarks.
for benchmark in benchmarks.keys():
    opt = opts[benchmark]
    for param in benchmarks[benchmark]:
        classname = "edu.rice.habanero.benchmarks." + benchmark

        # WRC
        filename = f"{benchmark}-n{param}-WRC"
        subprocess.run(
            ["sbt",
            f"-Duigc.engine=mac", "-Duigc.mac.cycle-detection=off",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])
        subprocess.run(f"jfr print --json {filename}.jfr > {filename}.json", shell=True)
        os.remove(f"{filename}.jfr")

        # MAC
        filename = f"{benchmark}-n{param}-MAC"
        subprocess.run(
            ["sbt",
            f"-Duigc.engine=mac", "-Duigc.mac.cycle-detection=on",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])
        subprocess.run(f"jfr print --json {filename}.jfr > {filename}.json", shell=True)
        os.remove(f"{filename}.jfr")

        # CRGC on-block
        filename = f"{benchmark}-n{param}-crgc-onblock"
        subprocess.run(
            ["sbt",
            f"-Dgc.crgc.collection-style=on-block", f"-Duigc.engine=crgc",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])
        subprocess.run(f"jfr print --json {filename}.jfr > {filename}.json", shell=True)
        os.remove(f"{filename}.jfr")

        # CRGC wave
        filename = f"{benchmark}-n{param}-crgc-wave"
        subprocess.run(
            ["sbt",
            f"-Dgc.crgc.collection-style=wave", f"-Duigc.engine=crgc",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])
        subprocess.run(f"jfr print --json {filename}.jfr > {filename}.json", shell=True)
        os.remove(f"{filename}.jfr")
