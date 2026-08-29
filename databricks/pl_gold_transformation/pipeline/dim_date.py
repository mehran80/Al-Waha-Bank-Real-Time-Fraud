from pyspark import pipelines as dp
from pyspark.sql.functions import (
    explode,
    sequence,
    lit,
    to_date,
    date_format,
    dayofmonth,
    month,
    quarter,
    year,
    dayofweek
    
)

# ==============================================================================
# 1. GOLD DIM DATE
# ==============================================================================

@dp.materialized_view(
    name = "gold_dim_date",
    cluster_by_auto = True
)

def gold_dim_date():
    
    df = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit("2020-01-01")),
                    to_date(lit("2031-12-31"))
                )
            ).alias("date")
        )
    )
    
    df = (df.withColumns({
        "date_key" : date_format("date", "yyyyMMdd").cast("int"),
        "day": dayofmonth("date"),
        "month": month("date"),
        "month_name": date_format("date", "MMMM"),
        "quarter": quarter("date"),
        "year": year("date"),
        "is_weekend": dayofweek("date").isin(1, 7)
    })
    .select(
        "date_key",
        "date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "is_weekend"
    )
    )
    return df
