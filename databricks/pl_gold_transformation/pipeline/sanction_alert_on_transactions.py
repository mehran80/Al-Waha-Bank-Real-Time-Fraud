from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.materialized_view(name = "sanction_alert_on_transaction")
def sanction_alert_on_transaction():
    df_sanction_hit = spark.read.table("alwaha_banking_dev_001.gold.dim_sanction_hits")
    df_transaction = spark.read.table("alwaha_banking_dev_001.gold.gold_fact_transactions")

    df = (
        df_sanction_hit.alias("s")
        .join(
            df_transaction.alias("t"),
            col("s.customer_id") == col("t.customer_id"),
            "inner"
            )
        .select(
            "s.customer_id",
            "s.full_name",
            "s.entry_id",
            "s.list_source",
            "t.transaction_id",
            "t.account_id"

        )
    )
    return df