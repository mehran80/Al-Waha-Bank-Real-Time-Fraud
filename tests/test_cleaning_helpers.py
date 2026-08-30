import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from utilities.cleaning_helpers import clean_account_currency, clean_formatted_account_id

def test_clean_account_currency_uppercases(spark):
    df = spark.createDataFrame([("aed",), ("usd",)], ["currency"])
    result = df.withColumn("clean_currency", clean_account_currency("currency"))
    values = [row["clean_currency"] for row in result.collect()]
    assert values == ["AED", "USD"]

def test_clean_formatted_account_id_trims_spaces(spark):
    df = spark.createDataFrame([("  ACC1234567  ",)], ["account_id"])
    result = df.withColumn("clean_id", clean_formatted_account_id("account_id"))
    assert result.collect()[0]["clean_id"] == "ACC1234567"