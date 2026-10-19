import os
import re
import csv
import matplotlib.pyplot as plt
from collections import defaultdict

# 1. Read the actual wall-clock times recorded by the bash script
actual_times = defaultdict(list)
with open("results/scaling_times.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        actual_times[int(row["workers"])].append(float(row["wall_clock_seconds"]))

# 2. Read the total busy time from the Prometheus text files
busy_times = defaultdict(float)
jobs_counts = defaultdict(int)
file_counts = defaultdict(int)
RESULTS_DIR = "results/scaling"

for filename in sorted(os.listdir(RESULTS_DIR)):
    if not filename.endswith(".txt"):
        continue
    match = re.search(r"workers-(\d+)", filename)
    if not match:
        continue
    workers = int(match.group(1))

    with open(os.path.join(RESULTS_DIR, filename), "r") as f:
        content = f.read()

    busy_match = re.search(r'rjq_job_processing_duration_seconds_sum\{queue="fifo"\} ([\d.]+)', content)
    count_match = re.search(r'rjq_job_processing_duration_seconds_count\{queue="fifo"\} (\d+)', content)

    if busy_match and count_match:
        busy_times[workers] += float(busy_match.group(1))
        jobs_counts[workers] += int(count_match.group(1))
        file_counts[workers] += 1

# 3. Calculate accurate metrics
workers_list = []
throughput_list = []
utilization_list = []

print(f"{'Workers':<8} | {'Throughput (j/s)':<16} | {'Utilization (%)':<15}")
print("-" * 45)

for workers in sorted(actual_times.keys()):
    avg_wall_clock = sum(actual_times[workers]) / len(actual_times[workers])
    avg_busy = busy_times[workers] / file_counts[workers]
    avg_jobs = jobs_counts[workers] / file_counts[workers]

    # Accurate calculations based on real wall-clock time
    throughput = avg_jobs / avg_wall_clock
    utilization = (avg_busy / (avg_wall_clock * workers)) * 100

    workers_list.append(workers)
    throughput_list.append(throughput)
    utilization_list.append(utilization)

    print(f"{workers:<8} | {throughput:<16.2f} | {utilization:<15.2f}")

# 4. Plot
plt.figure(figsize=(8, 5))
plt.plot(workers_list, throughput_list, marker="o", linewidth=2, color="#2563eb")
plt.title("System Throughput vs Worker Count")
plt.xlabel("Workers")
plt.ylabel("Throughput (jobs/sec)")
plt.xticks(workers_list)
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("results/fig1_throughput_accurate.png")

plt.figure(figsize=(8, 5))
plt.plot(workers_list, utilization_list, marker="s", linewidth=2, color="#16a34a")
plt.title("Worker Utilization vs Worker Count")
plt.xlabel("Workers")
plt.ylabel("Utilization (%)")
plt.ylim(0, 100)
plt.xticks(workers_list)
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("results/fig2_utilization_accurate.png")

print("\nSaved accurate graphs.")