import sys

sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from pyspark.sql.functions  import(
    col,
    window,
    collect_set,
    expr,
    current_timestamp,
    size
)

from transformations.gold_velocity_alerts_transforms import detect_velocity_alert

# ==============================================================================
# 1. GOLD VELOCITY ALERTS
# ==============================================================================

@dp.table(
    name = "gold_velocity_alerts"
)

def gold_velocity_alerts():

    swipes = spark.readStream.table("alwaha_banking_dev_001.silver.silver_card_swipes")

    return detect_velocity_alert(swipes)