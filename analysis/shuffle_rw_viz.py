# Visualizes the average shuffle read and write bytes for each query and API, with error bars representing the standard deviation across runs.
# The data is read from a JSON file containing stage metrics, and the resulting plot is saved as a PNG image.
# Visualized as a stacked bar chart, where the bottom portion represents shuffle read bytes and the top portion represents shuffle write bytes
# Grouped by query, subcategorized by API
import pandas as pd, json
import matplotlib.pyplot as plt
import os

with open("../results/scaling/pct=100/stage_metrics_merged.json") as f:
    data = json.load(f)

df = pd.DataFrame(data["records"])

grouped = df.groupby(["query", "api"])[["shuffle_read_bytes", "shuffle_write_bytes"]]
avg = grouped.mean() / (1024**2)
std = grouped.std() / (1024**2)

avg = avg.drop("temporal_aggregation", level="query")
std = std.drop("temporal_aggregation", level="query")

queries = avg.index.get_level_values("query").unique()
apis = ["RDD", "DataFrame", "SQL"]

x = range(len(queries))
width = 0.25
colors_read = {"RDD": "#4C72B0", "DataFrame": "#DD8452", "SQL": "#55A868"}
colors_write = {"RDD": "#7FA8D9", "DataFrame": "#F2B482", "SQL": "#88CFA0"}

fig, ax = plt.subplots(figsize=(12, 6))

for i, api in enumerate(apis):
    offsets = [xi + i * width for xi in x]
    reads = [avg.loc[(q, api), "shuffle_read_bytes"] for q in queries]
    writes = [avg.loc[(q, api), "shuffle_write_bytes"] for q in queries]
    reads_err = [std.loc[(q, api), "shuffle_read_bytes"] for q in queries]
    writes_err = [std.loc[(q, api), "shuffle_write_bytes"] for q in queries]
    total_err = [r + w for r, w in zip(reads_err, writes_err)]

    ax.bar(offsets, reads, width, label=f"{api} Read", color=colors_read[api])
    ax.bar(
        offsets,
        writes,
        width,
        label=f"{api} Write",
        color=colors_write[api],
        bottom=reads,
        yerr=total_err,
        capsize=4,
        error_kw={"elinewidth": 1.2, "ecolor": "black"},
    )

ax.set_xticks([xi + width for xi in x])
ax.set_xticklabels(queries, rotation=30, ha="right")
ax.set_xlabel("Query")
ax.set_ylabel("Shuffle Data (MB)")
ax.set_title("Average Shuffle Read + Write per Query and API")
ax.legend(title="API / Direction", bbox_to_anchor=(1.01, 1), loc="upper left")

plt.tight_layout()

os.makedirs("figures", exist_ok=True)
plt.savefig("figures/shuffle_rw_stacked.png", dpi=150, bbox_inches="tight")
plt.show()
