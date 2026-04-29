# Visualizes the average elapsed time for each query and API across different data scales
# The data is read from JSON files containing wall clock times for each percentage of data, and the resulting plot is saved as a PNG image.
# Visualized as a line chart with error bars, where x-axis represents the percentage of data, y-axis represents the average elapsed time in seconds
# 5 subplots, one for each query. Each subplot has a different line for each API
import json
import pandas as pd
import matplotlib.pyplot as plt
import os

pcts = [5, 10, 25, 50, 100]
apis = ["RDD", "DataFrame", "SQL"]
colors = {"RDD": "#4C72B0", "DataFrame": "#DD8452", "SQL": "#55A868"}

records = []
for pct in pcts:
    path = f"../../results/scaling/pct={pct}/allqueries_wall_clock_merged.json"
    with open(path) as f:
        data = json.load(f)
    for r in data["records"]:
        r["scale_pct"] = pct
    records.extend(data["records"])

df = pd.DataFrame(records)

grouped = df.groupby(["scale_pct", "query", "api"])["elapsed_sec"]
means = grouped.mean()
stds  = grouped.std()

queries = df["query"].unique()
fig, axes = plt.subplots(2, 3, figsize=(16, 9))
axes = axes.flatten()

for idx, query in enumerate(sorted(queries)):
    ax = axes[idx]
    for api in apis:
        y    = [means.loc[(pct, query, api)] for pct in pcts]
        yerr = [stds.loc[(pct, query, api)]  for pct in pcts]
        ax.errorbar(pcts, y, yerr=yerr, label=api, color=colors[api],
                    marker="o", capsize=4, linewidth=1.8)
    ax.set_title(query, fontsize=10)
    ax.set_xlabel("Data Scale (%)")
    ax.set_ylabel("Avg Elapsed Time (sec)")
    ax.set_xticks(pcts)
    ax.legend(title="API")

axes[-1].set_visible(False)

fig.suptitle("Wall Clock Scaling by Query and API", fontsize=14, y=1.02)
plt.tight_layout()

plt.savefig("../figures/wall_clock_scaling.png", dpi=150, bbox_inches="tight")
plt.show()