from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.materialized_view(name = "sanction_alert_on_swipes_card")

def sanction_alert_on_swipes_card():
    df_sanction_hit = spark.read.table("alwaha_banking_dev_001.gold.dim_sanction_hits")
    df_swipes_card = spark.readStream.table("alwaha_banking_dev_001.gold.gold_fact_transactions")

    df = (
        df_sanction_hit.alias("s")
        .join(
            df_swipes_card.alias("c"),
            col("s.customer_id") == col("c.customer_id"),
            "inner"
            )
        .select(
            "s.customer_id",
            "s.full_name",
            "s.entry_id",
            "s.list_source",
            "c.swipe_id",
            "c.account_id"

        )
    )
    return df