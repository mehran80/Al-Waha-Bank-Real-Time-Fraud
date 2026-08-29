import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from transformations.silver_accounts_transforms import (
    cleaned_accounts,
    accounts_validate,
    get_clean_validated_accounts,
    get_rejected_accounts
)

from utilities.expectations import ACCOUNT_EXPECTATIONS

# ==============================================================================
# 1. CLEAN ACCOUNTS
# ==============================================================================
@dp.temporary_view(
    name = "accounts_cleaned"
)
def accounts_cleaned():
    df =spark.read.table("alwaha_banking_dev_001.bronze.bronze_core_accounts")
    return cleaned_accounts(df)


# ==============================================================================
# 2.VALIDATED ACCOUNTS
# ==============================================================================

@dp.temporary_view(
    name = "validated_accounts"
)
def validated_accounts():
    df = spark.read.table("accounts_cleaned")
    df_customers = spark.read.table("alwaha_banking_dev_001.silver.silver_dim_customers")
    
    return accounts_validate(df, df_customers)

# ==============================================================================
# 3. CLEAN VALIDATED ACCOUNTS
# ==============================================================================

@dp.temporary_view(
    name = "clean_validated_accounts"
)

@dp.expect_all(ACCOUNT_EXPECTATIONS)

def clean_validated_accounts():
    df = spark.read.table("validated_accounts")
    
    return get_clean_validated_accounts(df)

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
    
    return get_rejected_accounts(df)
