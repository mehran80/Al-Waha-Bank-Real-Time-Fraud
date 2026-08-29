from pyspark.sql.functions  import(
    col,
    window,
    collect_set,
    expr,
    current_timestamp,
    size
)

def detect_velocity_alert(df):
    df = (
        df
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