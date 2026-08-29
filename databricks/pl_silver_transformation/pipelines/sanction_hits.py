from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws, sha2

# ==============================================================================
# 1. SILVER SANCTION HITS
# ==============================================================================

@dp.materialized_view(
    name = "silver_sanction_hits"
)

def silver_sanction_hits():

    customer = spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")
    sanction = spark.read.table("alwaha_banking_dev_001.silver.silver_sanctions_list")

    df = (
        customer.alias("c")
        .join(
            sanction.alias("s"),
            (col("c.full_name") == col("s.name")) &
            (col("c.date_of_birth") == col("s.date_of_birth")) &
            (col("c.nationality") == col("s.nationality")),
            "inner"
        )
        .select(
            sha2(concat_ws("||", col("c.customer_id"), col("s.entry_id")), 256).alias("hit_id"),
            "c.customer_id",
            "c.full_name"
            "s.entry_id",
            "s.list_source"
            
        )
    )
    return df