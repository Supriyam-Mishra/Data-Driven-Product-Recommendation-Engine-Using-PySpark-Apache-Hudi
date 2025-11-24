import sys
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
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

def add_dq_flags_competitor(df):
    df = df.withColumn(
        "dq_failure_reason",
        F.concat_ws(
            ",",
            F.array(
                F.when(F.col("item_id").isNull(), F.lit("missing_item_id")),
                F.when(F.col("seller_id").isNull(), F.lit("missing_seller_id")),
                F.when(F.col("units_sold") < 0, F.lit("units_sold_negative")),
                F.when(F.col("revenue") < 0, F.lit("revenue_negative")),
                F.when(F.col("marketplace_price") < 0, F.lit("price_negative")),
                F.when(F.col("sale_date").isNull(), F.lit("missing_sale_date")),
                F.when(F.col("sale_date") > F.current_date(), F.lit("future_sale_date"))
            )
        )
    )

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
    spark = get_spark_session("ETL_Competitor_Sales")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    input_path = cfg["competitor_sales"]["input_path"]
    hudi_output = cfg["competitor_sales"]["hudi_output_path"]
    quarantine_path = hudi_output + "/quarantine/competitor_sales"

    df = spark.read.option("header", True).csv(input_path)

    # Trim and normalize
    df = df.withColumn("item_id", F.trim("item_id")) \
           .withColumn("seller_id", F.trim("seller_id"))

    # Cast types
    df = df.withColumn("units_sold", F.col("units_sold").cast(IntegerType())) \
           .withColumn("revenue", F.col("revenue").cast(DoubleType())) \
           .withColumn("marketplace_price", F.col("marketplace_price").cast(DoubleType())) \
           .withColumn("sale_date", F.to_date("sale_date"))

    # Fill missing numeric with 0
    df = df.fillna({"units_sold": 0, "revenue": 0.0, "marketplace_price": 0.0})

    df = add_dq_flags_competitor(df)

    bad_df = df.filter(F.col("dq_failure_reason").isNotNull())
    good_df = df.filter(F.col("dq_failure_reason").isNull())


    bad_df.write.mode("overwrite").option("header", True).csv(quarantine_path)

    write_hudi(
        good_df,
        hudi_output,
        table_name="competitor_sales_hudi",
        record_key="seller_id,item_id"
    )

if __name__ == "__main__":
    config_path = sys.argv[1]
    main(config_path)
