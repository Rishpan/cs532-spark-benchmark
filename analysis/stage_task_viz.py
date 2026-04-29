# Visualizes the average number of stages and tasks for each query and AP
# The data is read from a JSON file containing stage metrics, and the resulting plot is saved as a PNG image.
# Visualized as two separated grouped bar charts, one for stages and one for tasks
# Grouped by query, subcategorized by API
import json
import pandas as pd
import matplotlib.pyplot as plt
import os

with open("../results/scaling/pct=100/stage_metrics_merged.json") as f:
    data = json.load(f)

df = pd.DataFrame(data["records"])

grouped = df.groupby(["query", "api"])[["num_stages", "num_tasks"]]
avg = grouped.mean()
std = grouped.std()

queries = avg.index.get_level_values("query").unique()
apis = ["RDD", "DataFrame", "SQL"]

x = range(len(queries))
width = 0.25
colors = {"RDD": "#4C72B0", "DataFrame": "#DD8452", "SQL": "#55A868"}

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

for i, api in enumerate(apis):
    offsets = [xi + i * width for xi in x]
    stages     = [avg.loc[(q, api), "num_stages"] for q in queries]
    tasks      = [avg.loc[(q, api), "num_tasks"]  for q in queries]
    stages_err = [std.loc[(q, api), "num_stages"] for q in queries]
    tasks_err  = [std.loc[(q, api), "num_tasks"]  for q in queries]

    ax1.bar(offsets, stages, width, label=api, color=colors[api],
            yerr=stages_err, capsize=4, error_kw={"elinewidth": 1.2, "ecolor": "black"})
    ax2.bar(offsets, tasks,  width, label=api, color=colors[api],
            yerr=tasks_err,  capsize=4, error_kw={"elinewidth": 1.2, "ecolor": "black"})

for ax in (ax1, ax2):
    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels(queries, rotation=30, ha="right")
    ax.legend(title="API")

ax1.set_ylabel("Number of Stages")
ax1.set_title("Average Stage & Task Count per Query and API")
ax2.set_ylabel("Number of Tasks")
ax2.set_xlabel("Query")

plt.tight_layout()

os.makedirs("figures", exist_ok=True)
plt.savefig("figures/stage_task_count.png", dpi=150, bbox_inches="tight")
plt.show()