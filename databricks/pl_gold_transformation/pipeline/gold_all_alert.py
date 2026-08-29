from pyspark import pipelines as dp
from pyspark.sql.functions import (
    expr
)
# ==============================================================================
# 1. ALL FRAUD ALERTS
# ==============================================================================

@dp.materialized_view(
    name = "gold_all_alerts"
)

def gold_all_alerts():

    velocity = spark.read.table("alwaha_banking_dev_001.gold.gold_velocity_alerts").withColumn("alert_type", expr("'velocity'"))
    rule_based = spark.read.table("alwaha_banking_dev_001.gold.gold_fraud_alerts").withColumn("alert_type", expr("'risk_hint'"))

    return rule_based.unionByName(velocity, allowMissingColumns=True)