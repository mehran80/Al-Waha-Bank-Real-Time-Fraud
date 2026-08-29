from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# ==============================================================================
# 1. GOLD FACT TRANSACTIONS 
# ==============================================================================

@dp.materialized_view(
    name = "gold_fact_transactions",
    cluster_by_auto = True
)
def gold_fact_transactions():
    df = spark.read.table("alwaha_banking_dev_001.silver.silver_transactions")
    df = df.withColumn("_gold_processed_at", current_timestamp())
    return df