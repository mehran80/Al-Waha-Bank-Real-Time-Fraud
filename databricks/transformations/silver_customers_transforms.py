import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark.sql.window import Window
from pyspark.sql.functions import ( 
        col,
        lit,
        to_timestamp,
        upper,
        trim,
        current_timestamp,
        array,
        when,
        filter as spark_filter,
        current_date,
        size,
        row_number
    )
from utilities.cleaning_helpers import (
    clean_name,
    clean_emirates_id,
    clean_email,
    clean_phone,
    clean_nationality,
    parse_mixed_date,
    clean_formatted_customer_id
)

# ==============================================================================
# 1. CLEANED CUSTOMERS 
# ==============================================================================

def customers_cleaned(df):
    df =df.drop("_rescued_data")
    df = df.withColumns({
        "customer_id": clean_formatted_customer_id("customer_id"),
        "full_name": clean_name("full_name"),
        "emirates_id": clean_emirates_id("emirates_id"),
        "email": clean_email("email"),
        "phone": clean_phone("phone"),
        "nationality": clean_nationality("nationality"),
        "date_of_birth" : parse_mixed_date("date_of_birth"),
        "account_opened_date": parse_mixed_date("account_opened_date"),
        "risk_tier": upper(trim(col("risk_tier"))),
        "adf_run_id": trim(col("adf_run_id")),
        "_ingested_at": to_timestamp("_ingested_at"),
        "_silver_processed_at": current_timestamp(),
        "source_file": trim(col("source_file"))
        })
    
    return df

# ==============================================================================
# 2. VALIDATED CUSTOMERS 
# ==============================================================================

def customers_validate(df):
    df = df.withColumn(
        "rejected_reasons",
        array(
            when(
                col("customer_id").isNull() |
                ~col("customer_id").rlike(r"^CUST[0-9]{6}$"),
                lit("INVALID_CUSTOMER_ID_FORMAT")
                ),
            when(
                col("full_name").isNull() |
                (trim(col("full_name")) == ""),
                lit("FULL_NAME_IS_NOT_PROVIDED")
                ),
            when(
                    col("emirates_id").isNull() |
                    (trim(col("emirates_id")) == ""),
                    lit("EMIRATES_ID_NOT_PROVIDED")
                ),
            when(
                col("emirates_id").isNotNull() &
                ~col("emirates_id").rlike(r"^[0-9]{3}-[0-9]{4}-[0-9]{7}-[0-9]{1}$"),
                lit("INVALID_EMIRATES_ID_FORMAT")
                ),
            when(
                col("nationality").isNull() |
                (trim(col("nationality")) == ""),
                lit("NATIONALIITY_IS_NOT_PROVIDED")
                ),
            when(
                col("date_of_birth").isNull(),
                lit("DATE_OF_BIRTH_NOT_PROVIDED")
                ),
            when(
                col("date_of_birth").isNotNull() &
                (col("date_of_birth") > current_date()),
                lit("DATE_OF_BIRTH_IN_FUTURE")
                ),
            when(
                col("account_opened_date").isNull(),
                lit("ACCOUNT_OPENED_DATE_NOT_PROVIDED")
            ),
            when(
                col("account_opened_date") > current_date(),
                lit("ACCOUNT_OPENED_DATE_IN_FUTURE")
            ),
            when(
                col("account_opened_date") < col("date_of_birth"),
                lit("ACCOUNT_OPENED_DATE_BEFORE_DOB")
            ),
            when(
                col("risk_tier").isNull() |
                ~col("risk_tier").isin("LOW", "MEDIUM", "HIGH"),
                lit("INVALID_RISK_TIER")
                ),
            when(
                col("email").isNotNull() &
                ~col("email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]+$"),
                lit("INVALID_EMAIL_FORMAT")
                ),
            when(
                col("phone").isNotNull() &
                ~col("phone").rlike(r"^[+]971-[0-9]{2}-[0-9]{3}-[0-9]{4}$"),
                lit("INVALID_PHONE_FORMAT")
                ),
            when(
                col("adf_run_id").isNull() | 
                (trim(col("adf_run_id")) == ""),
                lit("ADF_RUN_ID_IS_NOT_PROVIDED")
                ),
            when(
                col("_ingested_at").isNull(),
                lit("INGESTION_DATE_IS_NOT_PROVIDED")
                )
            )
        )
    df = df.withColumn(
        "rejected_reasons",
        spark_filter(
            "rejected_reasons",
            lambda x: x.isNotNull()
        )
    )
    return df

# ==============================================================================
# 3. CLEAN VALIDATED CUSTOMERS 
# ==============================================================================

def get_clean_validated_customers(df):
    df = (
        df
        .filter(
            size(col("rejected_reasons")) == 0
            )
        .drop("rejected_reasons")
    )

    window = (
        Window
        .partitionBy("customer_id")
        .orderBy(col("_ingested_at").desc(),
                 col("_ingested_at").desc())
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
# 4. REJECTED CUSTOMERS 
# ==============================================================================

def get_rejected_customers(df):

    df = (
        df
        .filter(
            size(col("rejected_reasons")) > 0
        )
        .withColumn(
            "rejected_at",
            current_timestamp()
        )
    )

    return df