import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    to_timestamp,
    when,
    array,
    filter as spark_filter,
    lit,
    initcap,
    current_timestamp,
    size,
    upper
)
from utilities.cleaning_helpers import(
    clean_formatted_account_id,
    clean_formatted_customer_id,
    clean_formatted_card_id,
    clean_account_currency
)

# ==============================================================================
# 1. CLEANED CARD SWIPES
# ==============================================================================

def swipes_card_cleaned(df):

    df = df.drop("_rescued_data")

    df = df.withColumns({
        "account_id" : clean_formatted_account_id("account_id"),
        "customer_id" : clean_formatted_customer_id("customer_id"),
        "card_id" : clean_formatted_card_id("card_id"),
        "card_swipe_amount" : regexp_replace(trim(col("amount")), r"[^0-9.-]", "").cast("decimal(18,2)"),
        "city": when(
            upper(trim(col("city"))).rlike(r"^[\p{L}]+([ .''-][\p{L}]+)*$"),
            upper(trim(col("city")))
            ).otherwise(lit(None)),
        "country" : trim(col("country")),
        "swipe_card_currency": clean_account_currency("currency"),
        "swipe_timestamp": to_timestamp(col("event_ts")),
        "merchant" : trim(initcap(col("merchant"))),
        "mcc_code": trim(col("mcc_code")).cast("int"),
        "swipe_id": trim(col("swipe_id")),
        "_ingested_at": to_timestamp(trim(col("ingested_at"))),
        "adf_run_id": trim(col("adf_run_id")),
        "source_file": trim(col("file_name")),
        "_silver_processed_at": current_timestamp()
    })
    return df

# ==============================================================================
# 2. Validated CARD SWIPES
# ==============================================================================

def swipes_card_validate(df, customers, accounts):

    df = (
        df.alias("s")
        .join(
            customers.alias("c"),
            col("s.customer_id") == col("c.customer_id"),
            "left"
        )
        .join(
            accounts.alias("a"),
            col("s.account_id") == col("a.account_id"),
            "left"
        )
        .select(
            "s.*",
            col("c.customer_id").alias("_matched_customer_id"),
            col("a.account_id").alias("_matched_account_id")
        )
    )

    df = df.withColumn(
        "rejected_reasons",
        array(
            when(
                col("swipe_id").isNull() |
                ~col("swipe_id").rlike(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
                lit("INVALID_SWIPE_ID")
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
                col("card_id").isNull() |
                ~col("card_id").rlike(r"^CARD([0-9]{6})$"),
                lit("INVALID_CARD_ID")
            ),
            when(
                col("country").isNull() |
                (trim(col("country")) == ""),
                lit("INVALID_COUNTRY")
            ),
            when(
                col("card_swipe_amount").isNull() |
                (col("card_swipe_amount") <= 0),
                lit("INVALID_CARD_SWIPE_AMOUNT")
            ),
            when(
                col("swipe_timestamp").isNull() |
                (col("swipe_timestamp") > current_timestamp()),
                lit("INVALID_TIMESTAMP")
            )
        )
    )

    df = (
        df.withColumn(
            "rejected_reasons",
            spark_filter(
                col("rejected_reasons"),
                lambda x: x.isNotNull()
            )
        )
    )

    return df


# ==============================================================================
# 3. SILVER STREAMING CARD SWIPES
# ==============================================================================

def get_valid_swipes_card(df):

    df = (
        df
        .filter(
            size(col("rejected_reasons")) == 0
        )
        .drop(
            "rejected_reasons",
            "event_ts",
            "currency",
            "amount",
            "_matched_customer_id",
            "_matched_account_id",
            "file_name",
            "ingested_at"
        )
    )
    
    
    return df

# ==============================================================================
# 4. DEDUPLICATES VALID SWIPES
# ==============================================================================

def get_deduplicates_valid_swipes(df):

    df = df.withWatermark("swipe_timestamp", "24 hours").dropDuplicates(["swipe_id"])

    return df

# ==============================================================================
# 5. REJECTED / ORPHAN CARD SWIPES
# ==============================================================================

def get_rejected_swipes(df):

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
            "event_ts",
            "currency",
            "amount",
            "_matched_customer_id",
            "_matched_account_id",
            "file_name",
            "ingested_at"
        )
    )
    return df