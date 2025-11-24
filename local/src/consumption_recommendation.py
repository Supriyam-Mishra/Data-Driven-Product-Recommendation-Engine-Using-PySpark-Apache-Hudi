import sys
import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

def main(config_path: str):
    spark = get_spark_session("Consumption_Recommendation")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    sc_path = cfg["recommendation"]["seller_catalog_hudi"]
    cs_path = cfg["recommendation"]["company_sales_hudi"]
    comp_path = cfg["recommendation"]["competitor_sales_hudi"]
    output_csv = cfg["recommendation"]["output_csv"]

    # Read Hudi tables
    seller = spark.read.format("hudi").load(sc_path)
    company = spark.read.format("hudi").load(cs_path)
    competitor = spark.read.format("hudi").load(comp_path)

    # Aggregate company sales per item
    company_agg = (
        company.groupBy("item_id")
               .agg(
                   F.sum("units_sold").alias("company_units_sold"),
                   F.sum("revenue").alias("company_revenue")
               )
    )

    # Join with seller catalog to get item_name, category
    item_dim = seller.select("item_id", "item_name", "category").dropDuplicates(["item_id"])

    company_with_cat = (
        company_agg.join(item_dim, "item_id", "left")
    )

    # Top 10 items per category by units sold
    w = Window.partitionBy("category").orderBy(F.desc("company_units_sold"))
    top_items_per_cat = (
        company_with_cat
        .withColumn("rank_in_category", F.row_number().over(w))
        .filter(F.col("rank_in_category") <= 10)
        .drop("rank_in_category")
    )

    # All sellers
    sellers = seller.select("seller_id").distinct()

    # Cross join to get (seller_id, top_items)
    seller_top_items = sellers.crossJoin(
        top_items_per_cat.select("item_id", "item_name", "category", "company_units_sold")
    )

    # Items already in seller catalog
    seller_items = seller.select("seller_id", "item_id").distinct()

    # Missing items: top items not in seller's catalog
    missing_items = seller_top_items.join(
        seller_items,
        on=["seller_id", "item_id"],
        how="left_anti"
    )

    # Competitor aggregation
    competitor_agg = (
        competitor.groupBy("item_id")
        .agg(
            F.sum("units_sold").alias("competitor_units_sold"),
            F.avg("marketplace_price").alias("competitor_market_price")
        )
    )

    # Company marketplace price (avg across sellers)
    company_price = (
        seller.groupBy("item_id")
        .agg(
            F.avg("marketplace_price").alias("company_market_price")
        )
    )

    # Seller counts (company + competitor)
    seller_count_company = seller.select("seller_id", "item_id").distinct()
    seller_count_comp = competitor.select("seller_id", "item_id").distinct()

    all_seller_items = seller_count_company.unionByName(seller_count_comp)
    seller_counts = (
        all_seller_items.groupBy("item_id")
        .agg(F.countDistinct("seller_id").alias("seller_count"))
    )

    # Total units sold = company + competitor
    total_units = (
        company_agg.join(competitor_agg, "item_id", "outer")
        .select(
            F.col("item_id"),
            (F.coalesce(F.col("company_units_sold"), F.lit(0)) +
             F.coalesce(F.col("competitor_units_sold"), F.lit(0))).alias("total_units_sold")
        )
    )

    # Join metrics to missing items
    rec = (
        missing_items
        .join(total_units, "item_id", "left")
        .join(seller_counts, "item_id", "left")
        .join(company_price, "item_id", "left")
        .join(competitor_agg.select("item_id", "competitor_market_price"), "item_id", "left")
    )

    # Expected units sold
    rec = rec.withColumn(
        "expected_units_sold",
        F.when(F.col("seller_count") > 0,
               F.col("total_units_sold") / F.col("seller_count")
        ).otherwise(F.lit(0.0))
    )

    # Market price (prefer company price, fallback to competitor)
    rec = rec.withColumn(
        "market_price",
        F.coalesce(F.col("company_market_price"), F.col("competitor_market_price"))
    )

    # Expected revenue
    rec = rec.withColumn(
        "expected_revenue",
        F.col("expected_units_sold") * F.col("market_price")
    )

    final_cols = [
        "seller_id",
        "item_id",
        "item_name",
        "category",
        "market_price",
        "expected_units_sold",
        "expected_revenue"
    ]

    (
        rec.select(*final_cols)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output_csv)
    )

if __name__ == "__main__":
    config_path = sys.argv[1]
    main(config_path)
