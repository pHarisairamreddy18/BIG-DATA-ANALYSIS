from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, min, max
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("BigDataAnalysisHeatmap").getOrCreate()

# Load dataset WITHOUT schema inference to avoid mis-parsing
df = spark.read.csv("marketing_campaign.csv", header=True, sep="\t", inferSchema=False)

# Print initial schema and sample data to understand the dataset structure
print("Initial schema (all columns as string):")
df.printSchema()
df.show(5, truncate=False)

# List of numeric columns to cast explicitly
numeric_columns = [
    "Income", "Kidhome", "Teenhome", "Recency", "MntWines", "MntFruits",
    "MntMeatProducts", "MntFishProducts", "MntSweetProducts", "MntGoldProds",
    "NumDealsPurchases", "NumWebPurchases", "NumCatalogPurchases",
    "NumStorePurchases", "NumWebVisitsMonth", "AcceptedCmp3", "AcceptedCmp4",
    "AcceptedCmp5", "AcceptedCmp1", "AcceptedCmp2", "Complain",
    "Z_CostContact", "Z_Revenue"
]

# Cast columns to appropriate numeric types: double for amounts, int for counts/flags
for col_name in numeric_columns:
    if col_name == "Income" or "Mnt" in col_name or "Z_" in col_name:
        df = df.withColumn(col_name, col(col_name).cast("double"))
    else:
        df = df.withColumn(col_name, col(col_name).cast("int"))

# Verify schema after casting
print("Schema after casting numeric columns:")
df.printSchema()

# Compute summary statistics: mean, stddev, min, max for each numeric column
summary_stats = df.select(
    *[mean(col_name).alias(f"{col_name}_mean") for col_name in numeric_columns],
    *[stddev(col_name).alias(f"{col_name}_stddev") for col_name in numeric_columns],
    *[min(col_name).alias(f"{col_name}_min") for col_name in numeric_columns],
    *[max(col_name).alias(f"{col_name}_max") for col_name in numeric_columns]
).collect()[0].asDict()

# Prepare a DataFrame to hold the summary statistics for heatmap plotting
stats_types = ['mean', 'stddev', 'min', 'max']
data = {stat: [] for stat in stats_types}

for col_name in numeric_columns:
    for stat in stats_types:
        key = f"{col_name}_{stat}"
        val = summary_stats.get(key)
        data[stat].append(val if val is not None else float('nan'))

heatmap_df = pd.DataFrame(data, index=numeric_columns)

# Print summary stats DataFrame (optional)
print("Summary statistics DataFrame:")
print(heatmap_df)

# Plot and save heatmap of the summary statistics
plt.figure(figsize=(14, 10))
sns.heatmap(heatmap_df, annot=True, fmt=".2f", cmap="YlGnBu")
plt.title("Heatmap of Statistical Summaries (mean, stddev, min, max)")
plt.ylabel("Columns")
plt.xlabel("Statistics")
plt.tight_layout()
plt.savefig("heatmap_summary.png", bbox_inches='tight', dpi=300)
plt.show()

# Stop Spark session
spark.stop()
