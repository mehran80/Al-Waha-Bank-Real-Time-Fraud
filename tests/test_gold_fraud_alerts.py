
from transformations.gold_fraud_alerts_transforms import (
    join_swipes_with_customer_risk, calculate_fraud_score
)

def test_fraud_risk_high_with_flag_hint(spark):
    swipes = spark.createDataFrame(
        [("SW001", "CUST001", True)],
        ["swipe_id", "customer_id", "_flag_hint"]
    )
    customers = spark.createDataFrame(
        [("CUST001", "HIGH")],
        ["customer_id", "risk_tier"]
    )

    joined = join_swipes_with_customer_risk(swipes, customers)
    result = calculate_fraud_score(joined)
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["risk_score"] == 0.6
    assert rows[0]["hint_score"] == 0.4
    assert rows[0]["fraud_score"] == 1.0

def test_fraud_risk_medium_with_flag_hint(spark):
    swipes = spark.createDataFrame(
        [("SW003", "CUST003", True)],
        ["swipe_id", "customer_id", "_flag_hint"]
    )
    customers = spark.createDataFrame(
        [("CUST003", "MEDIUM")],
        ["customer_id", "risk_tier"]
    )

    joined = join_swipes_with_customer_risk(swipes, customers)
    result = calculate_fraud_score(joined)
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["risk_score"] == 0.3
    assert rows[0]["hint_score"] == 0.4
    assert rows[0]["fraud_score"] == 0.7

def test_fraud_risk_low_without_flag_hint(spark):
    swipes = spark.createDataFrame(
        [("SW004", "CUST004", False)],
        ["swipe_id", "customer_id", "_flag_hint"]
    )
    customers = spark.createDataFrame(
        [("CUST004", "LOW")],
        ["customer_id", "risk_tier"]
    )

    joined = join_swipes_with_customer_risk(swipes, customers)
    result = calculate_fraud_score(joined)

    assert result.count() == 1

def test_fraud_risk_medium_without_flag_hint(spark):
    swipes = spark.createDataFrame(
        [("SW004", "CUST004", False)],
        ["swipe_id", "customer_id", "_flag_hint"]
    )
    customers = spark.createDataFrame(
        [("CUST004", "MEDIUM")],
        ["customer_id", "risk_tier"]
    )

    joined = join_swipes_with_customer_risk(swipes, customers)
    result = calculate_fraud_score(joined)
    assert result.count() == 0

def test_join_excludes_swipes_with_unknown_customer(spark):
    swipes = spark.createDataFrame(
        [("SW005", "CUST_UNKNOWN", True)],
        ["swipe_id", "customer_id", "_flag_hint"]
    )
    customers = spark.createDataFrame(
        [("CUST001", "HIGH")],
        ["customer_id", "risk_tier"]
    )
    joined = join_swipes_with_customer_risk(swipes, customers)
    assert joined.count() == 0
