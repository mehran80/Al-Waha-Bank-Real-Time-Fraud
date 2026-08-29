from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# ==============================================================================
# 1. GOLD DIMENSION CUSTOMERS 
# ==============================================================================

@dp.materialized_view(
    name = "gold_dim_customers",
    cluster_by_auto = True
)

def dim_customers():

    df = spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")

    df = (
        df.withColumn(
        "_gold_processed_at", current_timestamp()
        )
        .drop("account_opened_date")
    )

    return df

