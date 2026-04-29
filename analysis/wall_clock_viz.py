# Visualizes the average elapsed time for each query and API, with error bars representing the standard deviation across runs.
# The data is read from a JSON file containing wall clock times, and the resulting plot is saved as a PNG image.
# Visualized as a grouped bar chart, where each group corresponds to a query. Subcategory is API
import pandas as pd, json
import matplotlib.pyplot as plt

with open("../results/scaling/pct=100/allqueries_wall_clock_merged.json") as f:
    data = json.load(f)

df = pd.DataFrame(data["records"])
grouped = df.groupby(["query", "api"])["elapsed_sec"]
means = grouped.mean().round(3).unstack()[["RDD", "DataFrame", "SQL"]]
stds = grouped.std().round(3).unstack()[["RDD", "DataFrame", "SQL"]]

means.plot(kind="bar", figsize=(10, 6), yerr=stds, capsize=4)
plt.title("Average Elapsed Time by Query and API")
plt.xlabel("Query")
plt.ylabel("Average Elapsed Time (sec)")
plt.xticks(
    rotation=45, ha="right"
)  # help with algining, sinc query names are kind of long
plt.legend(title="API")
plt.tight_layout()
plt.savefig("figures/wall_clock_comparison.png", dpi=150)
plt.show()
