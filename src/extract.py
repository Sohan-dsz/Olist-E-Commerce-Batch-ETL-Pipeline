"""
Extract stage: reads the raw Olist CSVs into Spark DataFrames with explicit
schemas (never inferSchema on data you're about to write to a warehouse --
see README for why this bit me).
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)


def get_spark(app_name: str = "olist-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # local[*] uses all cores on the machine running it -- fine for laptop-scale
        # (~100k orders); swap for a real master URL if this ever moves to a cluster.
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  # see README: default 200 is
        # way oversized for a dataset this small and was creating thousands of tiny
        # output files during the aggregation stage.
        .getOrCreate()
    )


ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
])


def _read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")          # keep malformed rows instead of
        .option("columnNameOfCorruptRecord", "_corrupt_record")  # silently dropping them
        .schema(schema)
        .csv(path)
    )


def extract_all(spark: SparkSession, data_dir: str) -> dict:
    """Returns a dict of raw DataFrames keyed by table name."""
    return {
        "orders": _read_csv(spark, f"{data_dir}/olist_orders_dataset.csv", ORDERS_SCHEMA),
        "order_items": _read_csv(spark, f"{data_dir}/olist_order_items_dataset.csv", ORDER_ITEMS_SCHEMA),
        "payments": _read_csv(spark, f"{data_dir}/olist_order_payments_dataset.csv", PAYMENTS_SCHEMA),
        "customers": _read_csv(spark, f"{data_dir}/olist_customers_dataset.csv", CUSTOMERS_SCHEMA),
        "products": _read_csv(spark, f"{data_dir}/olist_products_dataset.csv", PRODUCTS_SCHEMA),
    }
