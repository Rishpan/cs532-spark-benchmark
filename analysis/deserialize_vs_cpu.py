# Visualizes the average executor deserialize time and CPU time for each query and API
# The data is read from a JSON file containing stage metrics, and the resulting plot is saved as a PNG image.
# Visualized as a stacked bar chart, where the bottom portion represents deserialize time and the top portion represents CPU time
# Grouped by query, subcategorized by API
import json
import pandas as pd
import matplotlib.pyplot as plt
import os

with open("../results/scaling/pct=100/stage_metrics_merged.json") as f:
    data = json.load(f)

df = pd.DataFrame(data["records"])

grouped = df.groupby(["query", "api"])[
    ["executor_deserialize_time_sec", "executor_cpu_time_sec"]
]
avg = grouped.mean()
std = grouped.std()

queries = avg.index.get_level_values("query").unique()
apis = ["RDD", "DataFrame", "SQL"]

x = range(len(queries))
width = 0.25
colors_deser = {"RDD": "#4C72B0", "DataFrame": "#DD8452", "SQL": "#55A868"}
colors_cpu = {"RDD": "#7FA8D9", "DataFrame": "#F2B482", "SQL": "#88CFA0"}

fig, ax = plt.subplots(figsize=(12, 6))

for i, api in enumerate(apis):
    offsets = [xi + i * width for xi in x]
    deser = [avg.loc[(q, api), "executor_deserialize_time_sec"] for q in queries]
    cpu = [avg.loc[(q, api), "executor_cpu_time_sec"] for q in queries]
    deser_err = [std.loc[(q, api), "executor_deserialize_time_sec"] for q in queries]
    cpu_err = [std.loc[(q, api), "executor_cpu_time_sec"] for q in queries]
    total_err = [d + c for d, c in zip(deser_err, cpu_err)]

    ax.bar(offsets, deser, width, label=f"{api} Deserialize", color=colors_deser[api])
    ax.bar(
        offsets,
        cpu,
        width,
        label=f"{api} CPU",
        color=colors_cpu[api],
        bottom=deser,
        yerr=total_err,
        capsize=4,
        error_kw={"elinewidth": 1.2, "ecolor": "black"},
    )

ax.set_xticks([xi + width for xi in x])
ax.set_xticklabels(queries, rotation=30, ha="right")
ax.set_xlabel("Query")
ax.set_ylabel("Time (sec)")
ax.set_title("Average Executor Deserialize vs CPU Time per Query and API")
ax.legend(title="API / Component", bbox_to_anchor=(1.01, 1), loc="upper left")

plt.tight_layout()
os.makedirs("figures", exist_ok=True)
plt.savefig("figures/deserialize_vs_cpu.png", dpi=150, bbox_inches="tight")
plt.show()
