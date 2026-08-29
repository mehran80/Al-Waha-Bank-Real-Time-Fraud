import sys

sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from transformations.gold_fraud_alerts_transforms import (
    join_swipes_with_customer_risk, calculate_fraud_score
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

    df = join_swipes_with_customer_risk(swipe_card, customer)
    df = calculate_fraud_score(df)
    return df