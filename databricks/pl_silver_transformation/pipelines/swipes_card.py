import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from transformations.silver_swipes_card_transforms import(
    swipes_card_cleaned, swipes_card_validate, get_valid_swipes_card, get_deduplicates_valid_swipes,
    get_rejected_swipes
)
from utilities.expectations import SWIPES_CARD_EXPECTATIONS

# ==============================================================================
# 1. CLEANED CARD SWIPES
# ==============================================================================

@dp.temporary_view(
    name = "cleaned_card_swipes"
)

def cleaned_card_swipes():

    df = spark.readStream.table("alwaha_banking_dev_001.bronze.stream_card_swipes")

    return swipes_card_cleaned(df)

# ==============================================================================
# 2. Validated CARD SWIPES
# ==============================================================================

@dp.temporary_view(
    name = "validated_card_swipes"
)

def validated_card_swipes():

    df = spark.readStream.table("cleaned_card_swipes")

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

    return swipes_card_validate(df, customers, accounts)


# ==============================================================================
# 3. SILVER STREAMING CARD SWIPES
# ==============================================================================

dp.create_streaming_table(
    name = "silver_card_swipes",
    cluster_by_auto=True,
    expect_all = SWIPES_CARD_EXPECTATIONS
)

@dp.append_flow(
    target = "silver_card_swipes"
)

def append_valid_card_swipes():
    df = spark.readStream.table("validated_card_swipes")

    df = get_valid_swipes_card(df)
    df = get_deduplicates_valid_swipes(df)
    
    return df
# ==============================================================================
# 4. REJECTED / ORPHAN CARD SWIPES
# ==============================================================================

dp.create_streaming_table(
    name = "rejected_card_swipes"
)

@dp.append_flow(
    target = "rejected_card_swipes"
)
def append_rejected_card_swipes():
    df = spark.readStream.table("validated_card_swipes")

    return get_rejected_swipes(df)