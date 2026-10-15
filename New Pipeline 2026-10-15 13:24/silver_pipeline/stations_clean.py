
from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, upper, initcap, to_date, when, lit

# ==================================================
# 1. CONFIGURATION
# ==================================================

CATALOG = spark.conf.get("catalog", "vp_vk_mkc20040528")
BRONZE_TABLE = f"{CATALOG}.bronze.stations"


# ==================================================
# 2. STATIONS STANDARDISATION
# ==================================================

@dp.table(
    name=f"{CATALOG}.silver.stations",
    comment="Standardised Silver stations table"
)
def stations():

    df = spark.readStream.table(BRONZE_TABLE)

    # S1: Trim station_id and convert to uppercase
    df = df.withColumn(
        "station_id",
        when(
            col("station_id").isNull() |
            (trim(col("station_id")) == ""),
            lit(None).cast("string")
        ).otherwise(upper(trim(col("station_id"))))
    )

    # S2: Trim station_name and capitalise each word
    df = df.withColumn(
        "station_name",
        initcap(trim(col("station_name")))
    )

    # S3: Trim city and capitalise each word
    df = df.withColumn(
        "city",
        initcap(trim(col("city")))
    )

    # S4: Trim site_type and convert to uppercase
    df = df.withColumn(
        "site_type",
        upper(trim(col("site_type")))
    )

    # S5: Convert opened_on to DATE
    df = df.withColumn(
        "opened_on",
        to_date(trim(col("opened_on")), "yyyy-MM-dd")
    )

    # Return only the required Silver columns
    return df.select(
        col("station_id").cast("string"),
        col("station_name").cast("string"),
        col("city").cast("string"),
        col("site_type").cast("string"),
        col("opened_on").cast("date")
    )
