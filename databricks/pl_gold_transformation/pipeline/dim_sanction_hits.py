from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.materialized_view(name = "dim_sanction_hits")
def dim_sanction_hit():
    df = spark.read.table("alwaha_banking_dev_001.silver.silver_sanction_hits")
    df.withColumn("_gold_processed_at", current_timestamp())

    return df