from pyspark.sql.functions import (
    col,
    current_timestamp,
    when
)

def join_swipes_with_customer_risk(swipes_df, customer_df):
    df = (
        swipes_df.alias("sw")
        .join(
            customer_df.alias("c"),
            col("sw.customer_id") == col("c.customer_id"),
            "inner"
        )
    )
    return df

def calculate_fraud_score(df):
    df = (
        df.withColumns({
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