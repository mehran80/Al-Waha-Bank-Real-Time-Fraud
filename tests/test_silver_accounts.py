
from datetime import date, timedelta
from transformations.silver_accounts_transforms import (
    accounts_validate, get_clean_validated_accounts, get_rejected_accounts
)

def valid_account_has_no_rejection(spark):
    accounts = spark.createDataFrame([
        ("ACC1234567", "CUST123456", "SAVINGS", "ACTIVE", "AED",
         date(2023, 1, 1), "RUN001", "2023-01-01T00:00:00", "file1.csv")
    ], ["account_id", "customer_id", "account_type", "account_status",
        "account_currency", "opened_date", "adf_run_id", "_ingested_at", "source_file"])

    customers = spark.createDataFrame([("CUST123456",)], ["customer_id"])

    result = accounts_validate(accounts, customers)
    row = result.collect()[0]
    assert len(row["rejected_reasons"]) == 0

def test_invalid_account_id_format_rejected(spark):
    accounts = spark.createDataFrame([
        ("BADID", "CUST123456", "SAVINGS", "ACTIVE", "AED",
         date(2023, 1, 1), "RUN001", "2023-01-01T00:00:00", "file1.csv")
    ], ["account_id", "customer_id", "account_type", "account_status",
        "account_currency", "opened_date", "adf_run_id", "_ingested_at", "source_file"])

    customers = spark.createDataFrame([("CUST123456",)], ["customer_id"])

    result = accounts_validate(accounts, customers)
    row = result.collect()[0]
    assert "INVALID_ACCOUNT_ID_FORMAT" in row["rejected_reasons"]

def customer_not_found_rejected(spark):
    accounts = spark.createDataFrame([
        ("ACC1234567", "CUST999999", "SAVINGS", "ACTIVE", "AED",
         date(2023, 1, 1), "RUN001", "2023-01-01T00:00:00", "file1.csv")
    ], ["account_id", "customer_id", "account_type", "account_status",
        "account_currency", "opened_date", "adf_run_id", "_ingested_at", "source_file"])

    customers = spark.createDataFrame([("CUST123456",)], ["customer_id"])
    result = accounts_validate(accounts, customers)
    row = result.collect()[0]
    assert "CUSTOMER_NOT_FOUND" in row["rejected_reasons"]


def test_future_opened_date_rejected(spark):

    accounts = spark.createDataFrame([
        ("ACC1234567", "CUST123456", "SAVINGS", "ACTIVE", "AED",
         date(2023, 1, 1), "RUN001", "2023-01-01T00:00:00", "file1.csv")
    ], ["account_id", "customer_id", "account_type", "account_status",
        "account_currency", "opened_date", "adf_run_id", "_ingested_at", "source_file"])

    customers = spark.createDataFrame([("CUST123456",)], ["customer_id"])

    result = accounts_validate(accounts, customers)
    row = result.collect()[0]
    assert "OPENED_DATE_IN_FUTURE" in row["rejected_reasons"]

def test_dedup_keeps_latest_ingested_record(spark):
    from pyspark.sql.functions import array

    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("rejected_reasons", ArrayType(StringType()), True),
    ])

    validated = spark.createDataFrame([
        ("ACC1234567", "CUST123456", "2023-01-01T00:00:00", "file1.csv", []),
        ("ACC1234567", "CUST123456", "2023-06-01T00:00:00", "file2.csv", []),
    ], schema=schema)

    result = get_clean_validated_accounts(validated)
    assert result.count() == 1
    assert result.collect()[0]["source_file"] == "file2.csv"

def test_account_rejected_only_returns_flagged_rows(spark):

    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("rejected_reasons", ArrayType(StringType()), True),
    ])

    validated = spark.createDataFrame([
        ("ACC1234567", []),
        ("ACC7654321", ["INVALID_ACCOUNT_ID_FORMAT"]),
    ], ["account_id", "rejected_reasons"])

    result = get_rejected_accounts(validated)
    assert result.count() == 1
    assert result.collect()[0]["account_id"] == "ACC7654321"