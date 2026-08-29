import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from transformations.silver_customers_transforms import (
    customers_cleaned, customers_validate, get_cleaned_validated_customers,
    get_rejected_customers
)
from utilities.expectations import CUSTOMER_EXPECTATIONS

# ==============================================================================
# 1. CLEANED CUSTOMERS 
# ==============================================================================
@dp.temporary_view(
    name = "cleaned_customers"
)
def cleaned_customers():
    df = spark.read.table("alwaha_banking_dev_001.bronze.bronze_core_customers")
    return customers_cleaned(df)

# ==============================================================================
# 2. VALIDATED CUSTOMERS 
# ==============================================================================

@dp.temporary_view(
    name = "validated_customers"
)


def validated_customers():

    df =spark.read.table("cleaned_customers")

    return customers_validate(df)
   
# ==============================================================================
# 3. CLEAN VALIDATED CUSTOMERS 
# ==============================================================================

@dp.temporary_view(
    name = "clean_validated_customers"
)

@dp.expect_all(CUSTOMER_EXPECTATIONS)
def clean_validated_customers():

    df = spark.read.table("validated_customers")

    return get_cleaned_validated_customers(df)

# ==============================================================================
# 4. SCD TYPE 2 CUSTOMERS TARGET
# ==============================================================================

dp.create_streaming_table(
    name = "silver_dim_customers",
    comment = "Customer dimension maintained as scd type 2 using snapshot comparison ",
    cluster_by_auto = True
)

dp.create_auto_cdc_from_snapshot_flow(
    target = "silver_dim_customers",
    source = "clean_validated_customers",
    keys = ["customer_id"],
    stored_as_scd_type = 2
)

# ==============================================================================
# 5. REJECTED CUSTOMERS 
# ==============================================================================

@dp.materialized_view(
    name = "rejected_customers"
)

def rejected_customers():

    df = spark.read.table("validated_customers")

    return get_rejected_customers(df)
