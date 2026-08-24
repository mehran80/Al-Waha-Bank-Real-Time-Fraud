from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    to_timestamp,
    trim,
    current_timestamp,
    lit,
    when,
    current_date,
    filter as spark_filter,
    array,
    size,
    row_number
    )
from utilities.cleaning_helpers import (
    parse_mixed_date,
    clean_account_currency,
    clean_formatted_customer_id,
    clean_formatted_account_id,
    clean_account_type,
    clean_account_status
)
from utilities.expectations import ACCOUNT_EXPECTATIONS

# ==============================================================================
# 1. CLEAN ACCOUNTS
# ==============================================================================
@dp.temporary_view(
    name = "accounts_cleaned"
)
def accounts_cleaned():
    df =(
        spark.read.table(
            "alwaha_banking_dev_001.bronze.bronze_core_accounts"
            )
            .drop("_rescued_data")
        )
    df = df.withColumns({
        "account_id": clean_formatted_account_id("account_id"),
        "customer_id": clean_formatted_customer_id("customer_id"),
        "account_type": clean_account_type("account_type"),
        "account_status": clean_account_status("status"),
        "account_currency": clean_account_currency("currency"),
        "opened_date": parse_mixed_date("opened_date"),
        "adf_run_id": trim(col("adf_run_id")),
        "_ingested_at": to_timestamp("_ingested_at"),
        "source_file": trim(col("source_file")),
        "_silver_processed_at": current_timestamp()
        })
    return df


# ==============================================================================
# 2.VALIDATED ACCOUNTS
# ==============================================================================

@dp.temporary_view(
    name = "validated_accounts"
)
def validated_accounts():
    df = (
        spark.read.table("accounts_cleaned")
    )
    customers = (
        spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")
        .select("customer_id")
        .dropDuplicates(["customer_id"])
        )
    
    df = (
        df.alias("a")
        .join(
            customers.alias("c"),
            col("a.customer_id") == col("c.customer_id"),
            "left"
            )
        .select
        (
            "a.*",
            col("c.customer_id").alias("_matched_customer_id")
        )
        )
    df = df.withColumn(
        "rejected_reasons",
        array(
            when(
                col("customer_id").isNull() |
                ~col("customer_id").rlike("^CUST[0-9]{6}$"),
                lit("INVALID_CUSTOMER_ID_FORMAT")
                ),
            when(
                col("customer_id").isNotNull() &
                col("_matched_customer_id").isNull(),
                lit("CUSTOMER_NOT_FOUND")
                ),
            when(
                col("account_id").isNull() |
                ~col("account_id").rlike("^ACC[0-9]{7}$"),
                lit("INVALID_ACCOUNT_ID_FORMAT")
                ),
            when(
                col("account_type").isNull() |
                ~col("account_type").isin("SALARY", "SAVINGS", "CREDIT_CARD", "CURRENT"),
                lit("INVALID_ACCOUNT_TYPE")
                ),
            when(
                col("account_status").isNull() |
                ~col("account_status").isin("ACTIVE", "DORMANT", "CLOSED"),
                lit("INVALID_ACCOUNT_STATUS")
                ),
            when(
                col("account_currency").isNull() |
                ~col("account_currency").isin("AED", "EUR", "USD"),
                lit("INVALID_ACCOUNT_CURRENCY")
                ),
            when(
                col("opened_date").isNull(),
                lit("OPENED_DATE_REQUIRED")
                ),
            when(
                col("opened_date") > current_date(),
                lit("OPENED_DATE_IN_FUTURE")
                ),
            when(
                col("adf_run_id").isNull() |
                (trim(col("adf_run_id")) == ""),
                lit("INVALID_ADF_RUN_ID")
                ),
            when(
                col("_ingested_at").isNull(),
                lit("INVALID_INGESTED_AT")
                )
                
        )
    )
    df = df.withColumn(
        "rejected_reasons",
        spark_filter(
            col("rejected_reasons"),
            lambda x: x.isNotNull()
        )
    )
    
    return df

# ==============================================================================
# 3. CLEAN VALIDATED ACCOUNTS
# ==============================================================================

@dp.temporary_view(
    name = "clean_validated_accounts"
)

@dp.expect_all(ACCOUNT_EXPECTATIONS)

def clean_validated_accounts():
    df = spark.read.table("validated_accounts")
    
    df = (
        df
        .filter(
            size(col("rejected_reasons")) == 0
            )
        .drop("_matched_customer_id","rejected_reasons", "currency", "status")
        )
    
    window = (
        Window
        .partitionBy("account_id")
        .orderBy(
            col("_ingested_at").desc(),
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
# 4. SCD TYPE 2 ACCOUNTS
# ==============================================================================

dp.create_streaming_table(
    name = "silver_dim_accounts",
    comment = "Accounts dimension maintained as a SCD TYPE 2 using snapshot flow",
    cluster_by_auto=True
)

dp.create_auto_cdc_from_snapshot_flow(
    target = "silver_dim_accounts",
    source = "clean_validated_accounts",
    keys = ["account_id"],
    stored_as_scd_type = 2
)

# ==============================================================================
# 5. REJECTED / ORPHAN ACCOUNTS
# ==============================================================================

@dp.materialized_view(
    name = "rejected_accounts"
)
def rejected_accounts():
    df = spark.read.table("validated_accounts")
    
    df = (
        df
        .filter(
            size(col("rejected_reasons")) > 0
            )
        .withColumn(
            "rejected_at",
            current_timestamp()
        )
        .drop(
            "currency",
            "status",
            "_matched_customer_id"
        )
    )
    
    return df
