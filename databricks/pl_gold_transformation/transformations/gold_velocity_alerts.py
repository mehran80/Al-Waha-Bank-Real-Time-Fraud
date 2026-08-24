from pyspark import pipelines as dp
from pyspark.sql.functions  import(
    col,
    window,
    collect_set,
    expr,
    current_timestamp,
    size
)

# ==============================================================================
# 1. GOLD VELOCITY ALERTS
# ==============================================================================

@dp.table(
    name = "gold_velocity_alerts"
)

def gold_velocity_alerts():

    swipes = spark.readStream.table("alwaha_banking_dev_001.silver.silver_card_swipes")

    df = (
        swipes
        .withWatermark("swipe_timestamp", "10 minutes")
        .groupBy(
            col("customer_id"),
            window("swipe_timestamp", "5 minutes")
        )
        .agg(
            collect_set(col("city")).alias("city_hit"),
            expr("count(*) as swipe_count")
        )
        .withColumn(
            "distinct_city_count",
            size(col("city_hit"))
        )
        .filter(col("distinct_city_count") >= 2)
        .select(
            col("customer_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("swipe_count"),
            col("distinct_city_count"),
            current_timestamp().alias("alert_timestamp")
        )
    )

    return df