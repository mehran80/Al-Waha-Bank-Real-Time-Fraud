from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    when
)

# ==============================================================================
# 1. GOLD FRAUD ALERTS
# ==============================================================================

@dp.table(
    name = "gold_fraud_alerts",
    comment = "Gold table containing fraud alerts"
)

def gold_fraud_alerts():

    swipe_card = spark.readStream.table("alwaha_banking_dev_001.silver.silver_card_swipes")
    customer = spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")

    df = (
        swipe_card.alias("sw")
        .join(
            customer.alias("c"),
            col("sw.customer_id") == col("c.customer_id"),
            "inner"
        )
        .withColumns({
            "risk_score":when(col("c.risk_tier") == 'HIGH', 0.6)
                .when(col("c.risk_tier") == 'MEDIUM', 0.3)
                .otherwise(0.1),
            "hint_score":
                when(col("sw._flag_hint") == True, 0.4)
                .otherwise(0.0),
            "fraud_score": (col("risk_score") + col("hint_score"))
        })
        .filter(
            col("fraud_score") >= 0.5
        )
        .select(
            "sw.swipe_id",
            "c.customer_id",
            "risk_score",
            "hint_score",
            "fraud_score",
            current_timestamp().alias("alert_timestamp")
        )
        
    )
    return df