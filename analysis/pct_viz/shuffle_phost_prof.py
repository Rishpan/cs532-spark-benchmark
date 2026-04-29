# Visualizes the average total shuffle read and write bytes for the "perhost_traffic_profiling" query across different data scales and APIs
# The data is read from JSON files containing stage metrics for each data scale, and the resulting plot is saved as a PNG image.
# Visualized as a line plot with error bars, where the x-axis represents the data scale percentage and the y-axis represents the total shuffle in MB
# Each line is an API
import pandas as pd, json
import matplotlib.pyplot as plt

pcts = [5, 10, 25, 50, 100]
apis = ["RDD", "DataFrame", "SQL"]
colors = {"RDD": "#4C72B0", "DataFrame": "#DD8452", "SQL": "#55A868"}

records = []
for pct in pcts:
    path = f"../../results/scaling/pct={pct}/stage_metrics_merged.json"
    with open(path) as f:
        data = json.load(f)
    for r in data["records"]:
        r["scale_pct"] = pct
    records.extend(data["records"])

df = pd.DataFrame(records)
df = df[df["query"] == "perhost_traffic_profiling"]

# Convert to MB
df["shuffle_read_bytes"]  = df["shuffle_read_bytes"]  / (1024 ** 2)
df["shuffle_write_bytes"] = df["shuffle_write_bytes"] / (1024 ** 2)
df["shuffle_total"]       = df["shuffle_read_bytes"] + df["shuffle_write_bytes"]

grouped = df.groupby(["scale_pct", "api"])["shuffle_total"]
means = grouped.mean()
stds  = grouped.std()

fig, ax = plt.subplots(figsize=(9, 5))

for api in apis:
    y    = [means.loc[(pct, api)] for pct in pcts]
    yerr = [stds.loc[(pct, api)]  for pct in pcts]
    ax.errorbar(pcts, y, yerr=yerr, label=api, color=colors[api],
                marker="o", capsize=4, linewidth=1.8)

ax.set_xticks(pcts)
ax.set_xlabel("Data Scale (%)")
ax.set_ylabel("Total Shuffle (MB)")
ax.set_title("Shuffle Scaling — perhost_traffic_profiling")
ax.legend(title="API")
plt.tight_layout()

plt.savefig("../figures/shuffle_scaling_perhost.png", dpi=150, bbox_inches="tight")
plt.show()