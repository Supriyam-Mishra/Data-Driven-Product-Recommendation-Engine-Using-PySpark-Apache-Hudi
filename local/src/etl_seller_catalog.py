import sys
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def add_dq_flags_seller(df):
    df = df.withColumn(
        "dq_failure_reason",
        F.concat_ws(
            ",",
            F.array(
                F.when(F.col("seller_id").isNull(), F.lit("missing_seller_id")),
                F.when(F.col("item_id").isNull(), F.lit("missing_item_id")),
                F.when(F.col("item_name").isNull(), F.lit("missing_item_name")),
                F.when(F.col("category").isNull(), F.lit("missing_category")),
                F.when(F.col("marketplace_price") < 0, F.lit("negative_price")),
                F.when(F.col("stock_qty") < 0, F.lit("negative_stock"))
            )
        )
    )

    # If dq_failure_reason is empty string → no issues
    df = df.withColumn(
        "dq_failure_reason",
        F.when(F.col("dq_failure_reason") == "", F.lit(None)).otherwise(F.col("dq_failure_reason"))
    )

    return df

def write_hudi(df, output_path, table_name, record_key):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.precombine.field": record_key.split(",")[0],
    }

    (
        df.write.format("hudi")
        .options(**hudi_options)
        .mode("overwrite")
        .save(output_path)
    )

def main(config_path: str):
    spark = get_spark_session("ETL_Seller_Catalog")

    # Read config
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    input_path = cfg["seller_catalog"]["input_path"]
    hudi_output = cfg["seller_catalog"]["hudi_output_path"]
    quarantine_path = hudi_output + "/quarantine/seller_catalog"

    # Read raw CSV
    df = spark.read.option("header", True).csv(input_path)

    # Trim string columns
    for col in ["seller_id", "item_id", "item_name", "category"]:
        df = df.withColumn(col, F.trim(F.col(col)))

    # Normalize casing
    df = df.withColumn("item_name", F.initcap("item_name")) \
           .withColumn("category", F.initcap("category"))

    # Cast types
    df = df.withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType())) \
           .withColumn("stock_qty", F.col("stock_qty").cast(IntegerType()))

    # Fill missing stock_qty with 0
    df = df.fillna({"stock_qty": 0})

    # Remove duplicates on (seller_id, item_id)
    df = df.dropDuplicates(["seller_id", "item_id"])

    # Add DQ flags
    df = add_dq_flags_seller(df)

    # Split clean vs quarantine
    bad_df = df.filter(F.col("dq_failure_reason").isNotNull())
    good_df = df.filter(F.col("dq_failure_reason").isNull())


    # Write quarantine (CSV)
    (
        bad_df.write.mode("overwrite")
        .option("header", True)
        .csv(quarantine_path)
    )

    # Write clean data to Hudi
    write_hudi(
        good_df,
        hudi_output,
        table_name="seller_catalog_hudi",
        record_key="seller_id,item_id"
    )

if __name__ == "__main__":
    # Usage: spark-submit etl_seller_catalog.py configs/ecomm_prod.yml
    config_path = sys.argv[1]
    main(config_path)
