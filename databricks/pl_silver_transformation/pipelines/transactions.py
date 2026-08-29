import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from transformations.silver_ransactions_transformations import (
    transactions_cleaned, transactions_validate, get_cleaned_validated_transactions, get_rejected_transactions
)
from utilities.expectations import TRANSACTION_EXPECTATIONS

# ==============================================================================
# 1. CLEANED TRANSACTIONS 
# ==============================================================================
@dp.temporary_view(
    name = "cleaned_transactions"
)
def cleaned_transactions():
    df = spark.read.table("alwaha_banking_dev_001.bronze.bronze_card_transactions")
    
    return transactions_cleaned(df)

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

    return transactions_validate(df, accounts, customers)

# ==============================================================================
# 3. CLEANED VALID TRANSACTION BATCH
# ==============================================================================

@dp.temporary_view(
    name = "valid_transaction_batch"
)

@dp.expect_all(TRANSACTION_EXPECTATIONS)
def valid_transaction_batch():

    df = spark.read.table("validated_transactions")

    return get_cleaned_validated_transactions(df)

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

    return get_rejected_transactions(df)