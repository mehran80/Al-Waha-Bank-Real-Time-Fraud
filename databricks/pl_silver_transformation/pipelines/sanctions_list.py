import sys
sys.path.append("/Workspace/Users/mehran8023@gmail.com/Al-Waha-Bank-Real-Time-Fraud/databricks")

from pyspark import pipelines as dp
from transformations.silver_sanctions_transforms import (
    sanctions_cleaned, sanctions_validate, get_cleaned_validated_sanctions, get_rejected_sanctions
)
from utilities.expectations import SANCTIONS_LIST_EXPECTATIONS

# ==============================================================================
# 1. CLEANED SANCTIONS LIST
# ==============================================================================

@dp.temporary_view(
    name = "cleaned_sanctions_list"
)

def cleaned_sanctions_list():

    df = spark.read.table("alwaha_banking_dev_001.bronze.bronze_sanctions_list")

    return sanctions_cleaned(df)

# ==============================================================================
# 2. VALIDATED SANCTIONS LIST
# ==============================================================================
@dp.temporary_view(
    name = "validated_sanctions_list"
)

def validated_sanctions_list():

    df = spark.read.table("cleaned_sanctions_list")

    return sanctions_validate(df)
# ==============================================================================
# 3. SILVER SANCTIONS LIST
# ==============================================================================
@dp.materialized_view(
    name = "silver_sanctions_list"
)

@dp.expect_all(SANCTIONS_LIST_EXPECTATIONS)
def silver_sanctions_list():

    df = spark.read.table("validated_sanctions_list")

    return get_cleaned_validated_sanctions(df)
# ==============================================================================
# 4. REJECTED SANCTIONS LIST
# ==============================================================================

@dp.materialized_view(
    name = "rejected_sanctions_list"
)

def rejected_sanctions_list():

    df = spark.read.table("validated_sanctions_list")
   
    return get_rejected_sanctions(df)