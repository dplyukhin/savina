#!/bin/bash
import subprocess
import os
import sys
import json

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

def count_messages(filename):
    subprocess.run(f"jfr print --json {filename}.jfr > {filename}.json", shell=True)
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
    with open(f'{filename}.csv', 'w') as f:
        f.write(f'{total_app_msgs}, {total_ctrl_msgs}')
        print(f'Wrote {filename}.csv')
    os.remove(f"{filename}.json")

# Check if there are any .jfr files in the directory. If so, abort.
for file in os.listdir('.'):
    if file.endswith('.jfr') or file.endswith('.json') or file.endswith('.csv'):
        print("There are .jfr, .json, or .csv files in the directory. Please delete or move them first. Aborting.")
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

        # MAC
        filename = f"{benchmark}-n{param}-MAC"
        subprocess.run(
            ["sbt",
            f"-Duigc.engine=mac", "-Duigc.mac.cycle-detection=on",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])

        # CRGC on-block
        filename = f"{benchmark}-n{param}-crgc-onblock"
        subprocess.run(
            ["sbt",
            f"-Dgc.crgc.collection-style=on-block", f"-Duigc.engine=crgc",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])

        # CRGC wave
        filename = f"{benchmark}-n{param}-crgc-wave"
        subprocess.run(
            ["sbt",
            f"-Dgc.crgc.collection-style=wave", f"-Duigc.engine=crgc",
            f'runMain {classname} -iter {iter} -jfr-filename {filename}.jfr {opt} {param}'])

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
