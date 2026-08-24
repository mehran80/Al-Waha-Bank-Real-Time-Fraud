from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    upper,
    trim,
    current_timestamp,
    array,
    filter as spark_filter,
    size,
    regexp_replace,
    initcap,
    when,
    expr,
    row_number
    )
from utilities.cleaning_helpers import (
   clean_formatted_transaction_id,
   clean_formatted_card_id,
   clean_formatted_customer_id,
   clean_formatted_account_id,
   clean_account_currency,
)
from utilities.expectations import TRANSACTION_EXPECTATIONS

# ==============================================================================
# 1. CLEANED TRANSACTIONS 
# ==============================================================================
@dp.temporary_view(
    name = "cleaned_transactions"
)
def cleaned_transactions():
    df = (
        spark.read.table("alwaha_banking_dev_001.bronze.bronze_card_transactions")
        .drop("_rescued_data")
        )
    df = df.withColumns({
        "transaction_id": clean_formatted_transaction_id("transaction_id"),
        "card_id": clean_formatted_card_id("card_id"),
        "customer_id": clean_formatted_customer_id("customer_id"),
        "account_id": clean_formatted_account_id("account_id"),
        "merchant" : trim(initcap(col("merchant"))),
        "mcc_code": trim(col("mcc_code")).cast("int"),
        "transaction_amount" : regexp_replace(trim(col("amount")), r"[^0-9.-]", "").cast("decimal(18,2)"),
        "account_currency" : clean_account_currency("currency"),
        "city": when(
            upper(trim(col("city"))).rlike(r"^[\p{L}]+([ .''-][\p{L}]+)*$"),
            upper(trim(col("city")))
            ).otherwise(lit(None)),
        "transaction_timestamp": to_timestamp(trim(col("timestamp"))),
        "auth_code": upper(trim("auth_code")),
        "transaction_status": upper(trim(col("status"))),
        "_ingested_at": to_timestamp(trim(col("_ingested_at"))),
        "adf_run_id": trim(col("adf_run_id")),
        "source_file": trim(col("source_file")),
        "_silver_processed_at": current_timestamp()
        })
    return df

# ==============================================================================
# 2. VALIDATED TRANSACTIONS TABLE
# ==============================================================================

@dp.temporary_view(
    name = "validated_transactions"
)
def validated_transactions():
    df = spark.read.table("cleaned_transactions")

    customers = (
        spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")
        .select("customer_id")
        .dropDuplicates(["customer_id"])
    )

    accounts = (
        spark.read.table("alwaha_banking_dev_001.silver.silver_dim_accounts")
        .select("account_id")
        .dropDuplicates(["account_id"])
    )

    df = (
        df.alias("t")
        .join(
            customers.alias("c"),
            col("t.customer_id") == col("c.customer_id"),
            "left"
            )
        .join(
            accounts.alias("a"),
            col("t.account_id") == col("a.account_id"),
            "left"
            )
        .select(
            "t.*",
            col("c.customer_id").alias("_matched_customer_id"),
            col("a.account_id").alias("_matched_account_id")
        )
    )

    df = df.withColumn(
        "rejected_reasons",
        array(
            when(
                col("transaction_id").isNull() |
                ~col("transaction_id").rlike(r"^TXN([0-9]{14})$"),
                lit("INVALID_TRANSACTION_ID")
            ),
            when(
                col("card_id").isNull() |
                ~col("card_id").rlike(r"^CARD([0-9]{6})$"),
                lit("INVALID_CARD_ID")
            ),
            when(
                col("customer_id").isNull() |
                ~col("customer_id").rlike(r"^CUST([0-9]{6})$"),
                lit("INVALID_CUSTOMER_ID")
            ),
            when(
                col("customer_id").isNotNull() &
                col("_matched_customer_id").isNull(),
                lit("CUSTOMER_ID_DOES_NOT_EXIST")
            ),
            when(
                col("account_id").isNull() |
                ~col("account_id").rlike(r"^ACC([0-9]{7})$"),
                lit("INVALID_ACCOUNT_ID")
            ),
            when(
                col("account_id").isNotNull() &
                col("_matched_account_id").isNull(),
                lit("ACCOUNT_ID_DOES_NOT_EXIST")
            ),
            when(
                col("transaction_amount").isNull() |
                (col("transaction_amount") <= 0),
                lit("INVALID_TRANSACTION_AMOUNT")
            ),
            when(
                col("transaction_timestamp").isNull() |
                (col("transaction_timestamp") > current_timestamp()),
                lit("INVALID_TRANSACTION_TIMESTAMP")
            ),
            when(
                (col("transaction_status") == 'APPROVED') &
                (col("auth_code").isNull() |
                 ~col("auth_code").rlike(r"^AUTH[0-9]{6}$")),
                lit("INVALID_AUTH_CODE")
            ),
            when(
                col("transaction_status").isNull() |
                ~col("transaction_status").isin("APPROVED", "DECLINED"),
                lit("INVALID_TRANSACTION_STATUS")
            )
           
        )
    )
    df = (
        df.withColumn(
            "rejected_reasons",
            spark_filter(
                "rejected_reasons",
                lambda x: x.isNotNull()
            )
        )
    )
    df =( df.withColumn(
        "is_late_arrival",
         col("_ingested_at") > 
         (col("transaction_timestamp") + expr("INTERVAL 24 HOURS"))
        )
    )
    return df

# ==============================================================================
# 3. VALID TRANSACTION BATCH
# ==============================================================================

@dp.temporary_view(
    name = "valid_transaction_batch"
)

@dp.expect_all(TRANSACTION_EXPECTATIONS)
def valid_transaction_batch():
    df = spark.read.table("validated_transactions")

    df = (
        df
        .filter(
            size(col("rejected_reasons")) == 0
        )
        .drop(
            "rejected_reasons", "_matched_customer_id", "_matched_account_id", "currency", "status", "timestamp"
        )
    )

    window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(col("_ingested_at").desc(),
                 col("source_file").desc()
                )
    )
    df = (df.withColumn(
        "_rn", row_number().over(window)
        )
        .filter(
            col("_rn") == 1
        )
        .drop("_rn")
    )

    return df

# ==============================================================================
# 4. SILVER TRANSACTIONS
# ==============================================================================

dp.create_streaming_table(
    name = "silver_transactions",
    cluster_by_auto=True
)

dp.create_auto_cdc_from_snapshot_flow(
    target = "silver_transactions",
    source = "valid_transaction_batch",
    keys = ["transaction_id"],
    stored_as_scd_type = 1
)
    
# ==============================================================================
# 5. REJECTED / ORPHANS TRANSACTIONS
# ==============================================================================

@dp.materialized_view(
    name = "rejected_transactions"
)

def rejected_transactions():
    df = spark.read.table("validated_transactions")

    df = (
        df
        .filter(
            size(col("rejected_reasons")) > 0
        )
        .withColumn(
            "rejected_at", current_timestamp()
        )
        .drop("_matched_customer_id", "_matched_account_id", "currency", "status", "timestamp")
    )

    return df