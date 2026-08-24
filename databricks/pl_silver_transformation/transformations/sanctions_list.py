from pyspark import pipelines as dp
from pyspark.sql.functions import(
    col,
    trim,
    from_json,
    array_distinct,
    upper,
    transform,
    current_timestamp,
    to_timestamp,
    array,
    filter as spark_filter,
    lit,
    when,
    initcap,
    size
)
from utilities.cleaning_helpers import (
    parse_mixed_date
)
from utilities.expectations import SANCTIONS_LIST_EXPECTATIONS

# ==============================================================================
# 1. CLEANED SANCTIONS LIST
# ==============================================================================

@dp.temporary_view(
    name = "cleaned_sanctions_list"
)

def cleaned_sanctions_list():
    df = (
        spark.read.table("alwaha_banking_dev_001.bronze.bronze_sanctions_list")
        .drop("_rescued_data")
    )
    df = df.withColumns({
        "aliases": array_distinct(
            transform(
                from_json(col("aliases"), "ARRAY<STRING>"),
                lambda x: initcap(trim(x))
            )
        ),
        "date_of_birth": parse_mixed_date("date_of_birth"),
        "entity_type": upper(trim(col("entity_type"))),
        "entry_id": trim(col("entry_id")),
        "list_source": trim(col("list_source")),
        "name": initcap(trim(col("name"))),
        "nationality": trim(col("nationality")),
        "_ingested_at": to_timestamp(trim(col("_ingested_at"))),
        "adf_run_id": trim(col("adf_run_id")),
        "source_file": trim(col("source_file")),
        "_silver_processed_at": current_timestamp()
        
    })

    return df

# ==============================================================================
# 2. VALIDATED SANCTIONS LIST
# ==============================================================================
@dp.temporary_view(
    name = "validated_sanctions_list"
)

def validated_sanctions_list():
    df = spark.read.table("cleaned_sanctions_list")
    df = (
        df.withColumn(
            "rejected_reasons",
            array(
                when(
                    col("entity_type").isNull(),
                    lit("INVALID_ENTITY_TYPE")
                ),
                when(
                    col("list_source").isNull(),
                    lit("INVALID_LIST_SOURCE")
                ),
                when(
                    col("name").isNull(),
                    lit("INVALID_NAME")
                ),
                when(
                    col("entry_id").isNull() |
                    ~col("entry_id").rlike(r"^SANC[0-9]{4,}$"),
                    lit("INVALID_ENTRY_ID")
                )
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
# 3. SILVER SANCTIONS LIST
# ==============================================================================
@dp.materialized_view(
    name = "silver_sanctions_list"
)

@dp.expect_all(SANCTIONS_LIST_EXPECTATIONS)
def silver_sanctions_list():
    df = spark.read.table("validated_sanctions_list")
    df = (
        df
        .filter(
            size(col("rejected_reasons")) == 0
        )
        .drop("rejected_reasons")
        .dropDuplicates(["entry_id"])
    )
    return df
# ==============================================================================
# 4. REJECTED SANCTIONS LIST
# ==============================================================================

@dp.materialized_view(
    name = "rejected_sanctions_list"
)

def rejected_sanctions_list():
    df = spark.read.table("validated_sanctions_list")
    df = (
        df
        .filter(
            size(col("rejected_reasons")) > 0
        )
        .withColumn(
            "rejected_at",
            current_timestamp()
        )
        .dropDuplicates(["entry_id"])
    )
    return df