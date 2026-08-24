from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# ==============================================================================
# 1. GOLD FACT TRANSACTIONS 
# ==============================================================================

@dp.table(
    name = "gold_fact_card_swipes",
    cluster_by_auto = True
)

def gold_fact_card_swipes():
    return (
        spark.readStream.table("alwaha_banking_dev_001.silver.silver_card_swipes")
        .withColumn("_gold_processed_at", current_timestamp())
    )