from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Catalog parameter
CATALOG = spark.conf.get("catalog", "vp_vk_mkc20040528")

# Source and Auto Loader state paths
BASE_PATH = f"/Volumes/{CATALOG}/landing/raw/sessions"
STATE_PATH = f"/Volumes/{CATALOG}/ops/pipeline_state/sessions"


@dp.table(
    name="sessions",
    comment="Bronze sessions ingested using Auto Loader"
)
def sessions():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{STATE_PATH}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(BASE_PATH)
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file"),
            current_timestamp().alias("_ingested_at"),
        )
    )