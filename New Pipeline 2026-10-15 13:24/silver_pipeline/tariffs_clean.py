from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, upper

# ==================================================
# 1. CONFIGURATION
# ==================================================

CATALOG = spark.conf.get("catalog", "vp_vk_mkc20040528")
BRONZE_TABLE = f"{CATALOG}.bronze.tariffs"


# ==================================================
# 2. TARIFFS STANDARDISATION
# ==================================================

@dp.table(
    name=f"{CATALOG}.silver.tariffs",
    comment="Standardised Silver tariffs table"
)
def tariffs():

    df = spark.readStream.table(BRONZE_TABLE)

    # Connector type: trim and uppercase
    df = df.withColumn(
        "connector_type",
        upper(trim(col("connector_type")))
    )

    # Time band: trim and uppercase
    df = df.withColumn(
        "time_band",
        upper(trim(col("time_band")))
    )

    # Cast numeric columns
    df = (
        df.withColumn(
            "rate_per_kwh",
            col("rate_per_kwh").cast("decimal(10,2)")
        )
        .withColumn(
            "idle_fee_per_min",
            col("idle_fee_per_min").cast("decimal(10,2)")
        )
        .withColumn(
            "free_idle_minutes",
            col("free_idle_minutes").cast("int")
        )
    )

    # Return only required Silver columns; exclude metadata
    return df.select(
        col("connector_type").cast("string"),
        col("time_band").cast("string"),
        col("rate_per_kwh").cast("decimal(10,2)"),
        col("idle_fee_per_min").cast("decimal(10,2)"),
        col("free_idle_minutes").cast("int")
    )
