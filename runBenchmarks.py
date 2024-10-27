import subprocess
import sys
import os

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

# Create the raw data directory if it doesn't exist.
if not os.path.exists(raw_data_dir):
    os.makedirs(raw_data_dir)

# Check if there are any .csv files in the directory. If the "--append" flag is not used, abort.
for file in os.listdir('.'):
    if file.endswith('.csv') and "--append" not in sys.argv:
        print("There are .csv files in the directory. Either remove them or re-run with the --append flag. Aborting.")
        sys.exit()

# Run benchmarks, measuring execution time.
def run_time_benchmarks():
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

