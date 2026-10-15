
from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, trim, upper, when, lit, coalesce,
    try_to_timestamp, to_date, hour,
    unix_timestamp, floor, current_timestamp,
    transform 
)

# ==================================================
# 1. CONFIGURATION
# ==================================================

CATALOG = spark.conf.get("catalog", "vp_vk_mkc20040528")
BRONZE_TABLE = f"{CATALOG}.bronze.sessions"

# ==================================================
# 2. SESSION STANDARDISATION
# ==================================================

@dp.table(
    name=f"{CATALOG}.silver.sessions_standardized",
    comment="Standardised Bronze sessions data"
)
def sessions_standardized():

    df = spark.readStream.table(BRONZE_TABLE)

    # S1: Trim and uppercase IDs; empty values become NULL
    for c in ["session_id", "charger_id", "member_id"]:
        df = df.withColumn(
            c,
            when(
                col(c).isNull() | (trim(col(c).cast("string")) == ""),
                lit(None).cast("string")
            ).otherwise(upper(trim(col(c).cast("string"))))
        )

    # S2: Standardise status
    df = df.withColumn(
        "status",
        upper(trim(col("status")))
    )

    # S3: Parse only the two permitted timestamp formats
    start_ts = coalesce(
        try_to_timestamp(col("started_at"), lit("yyyy-MM-dd HH:mm:ss")),
        try_to_timestamp(col("started_at"), lit("yyyy-MM-dd'T'HH:mm:ss"))
    )

    end_ts = coalesce(
        try_to_timestamp(col("ended_at"), lit("yyyy-MM-dd HH:mm:ss")),
        try_to_timestamp(col("ended_at"), lit("yyyy-MM-dd'T'HH:mm:ss"))
    )

    df = (
        df.withColumn("started_at", start_ts)
          .withColumn("ended_at", end_ts)
          .withColumn("session_date", to_date(col("started_at")))
    )

    # S4: Calculate energy consumed from nested meter fields
    df = df.withColumn(
        "energy_kwh",
        when(
            col("meter.start_kwh").isNotNull()
            & col("meter.end_kwh").isNotNull(),
            col("meter.end_kwh") - col("meter.start_kwh")
        ).otherwise(lit(None).cast("double"))
    )

    # S5: Whole duration in minutes, rounded down
    df = df.withColumn(
        "duration_minutes",
        when(
            col("started_at").isNotNull()
            & col("ended_at").isNotNull(),
            floor(
                (
                    unix_timestamp(col("ended_at"))
                    - unix_timestamp(col("started_at"))
                ) / 60
            ).cast("long")
        ).otherwise(lit(None).cast("long"))
    )

    # S6: Peak hours are 18:00 to 21:59
    df = df.withColumn(
        "time_band",
        when(
            hour(col("started_at")).between(18, 21),
            lit("PEAK")
        ).otherwise(lit("OFF_PEAK"))
    )

    # S7: Missing idle_minutes becomes 0
    df = df.withColumn(
        "idle_minutes",
        coalesce(col("idle_minutes"), lit(0))
    )

    # S7: Standardise nested fault codes
    df = df.withColumn(
        "faults",
        transform(
            col("faults"),
            lambda f: f.withField(
                "code",
                upper(trim(f["code"]))
            )
        )
    )

    # C2: Keep the latest record per session_id
    window_spec = Window.partitionBy("session_id").orderBy(
        col("updated_at").desc_nulls_last()
    )

    df = (
        df.withColumn("_rn", row_number().over(window_spec))
          .filter(col("_rn") == 1)
          .drop("_rn")
        )

    # Cast fields and select the required Silver output columns
    return df.select(
        col("started_at").cast("timestamp"),
        col("session_id").cast("string"),
        col("charger_id").cast("string"),
        col("ended_at").cast("timestamp"),
        col("member_id").cast("string"),
        col("session_date").cast("date"),
        col("status").cast("string"),
        col("energy_kwh").cast("decimal(10,3)"),
        col("time_band").cast("string"),
        col("duration_minutes").cast("int"),
        col("idle_minutes").cast("int"),
        col("payment.amount_inr").cast("decimal(10,2)").alias("amount_paid"),
        col("updated_at").cast("timestamp")
    )
