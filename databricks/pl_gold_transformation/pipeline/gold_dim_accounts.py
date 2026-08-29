from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# ==============================================================================
# 1. GOLD DIMENSION CUSTOMERS 
# ==============================================================================

@dp.materialized_view(
    name = "gold_dim_accounts",
    cluster_by_auto = True
)

def dim_accounts():
    df = spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")

    df = df.withColumn("_gold_processed_at", current_timestamp())

    return df