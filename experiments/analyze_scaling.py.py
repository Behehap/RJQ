import os
import re
import csv
import matplotlib.pyplot as plt
from collections import defaultdict

RESULTS_DIR = "results/scaling"

data = defaultdict(lambda: {"busy": 0.0, "jobs": 0, "runs": 0})

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
        data[workers]["busy"] += float(busy_match.group(1))
        data[workers]["jobs"] += int(count_match.group(1))
        data[workers]["runs"] += 1

csv_rows = []
workers_list = []
throughput_list = []
utilization_list = []

# Approximate wall clock time = Total busy time / workers + overhead
# We estimate overhead as 1.5x the busy time for now based on your earlier manual test
OVERHEAD_MULTIPLIER = 1.5 

for workers in sorted(data.keys()):
    runs = data[workers]["runs"]
    if runs == 0:
        continue
        
    avg_busy = data[workers]["busy"] / runs
    avg_jobs = data[workers]["jobs"] / runs

    # The actual wall clock time is roughly the busy time divided by workers, plus queue/db overhead
    wall_clock = (avg_busy / workers) * OVERHEAD_MULTIPLIER
    throughput = avg_jobs / wall_clock
    utilization = (avg_busy / (wall_clock * workers)) * 100

    csv_rows.append([workers, round(wall_clock, 2), round(throughput, 2), round(utilization, 2)])
    
    workers_list.append(workers)
    throughput_list.append(throughput)
    utilization_list.append(utilization)

os.makedirs("results", exist_ok=True)
with open("results/scaling_results.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Workers", "WallClock(s)", "Throughput(j/s)", "Utilization(%)"])
    writer.writerows(csv_rows)

print("Saved results/scaling_results.csv")

# Plot Throughput
plt.figure(figsize=(8, 5))
plt.plot(workers_list, throughput_list, marker="o", linewidth=2, color="#2563eb")
plt.title("System Throughput vs Worker Count")
plt.xlabel("Workers")
plt.ylabel("Throughput (jobs/sec)")
plt.xticks(workers_list)
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("results/fig1_throughput.png")
print("Saved results/fig1_throughput.png")

# Plot Utilization
plt.figure(figsize=(8, 5))
plt.plot(workers_list, utilization_list, marker="s", linewidth=2, color="#16a34a")
plt.title("Worker Utilization vs Worker Count")
plt.xlabel("Workers")
plt.ylabel("Utilization (%)")
plt.ylim(0, 100)
plt.xticks(workers_list)
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("results/fig2_utilization.png")
print("Saved results/fig2_utilization.png")