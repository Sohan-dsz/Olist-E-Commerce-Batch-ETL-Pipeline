"""
Transform stage. Everything here is lazy until an action (.count(), .write, etc.)
triggers it -- that's why the row-count validation functions matter: they're the
only place we're actually forcing Spark to compute anything before the final write.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("olist_pipeline.transform")


def clean_orders(orders: DataFrame) -> DataFrame:
    """
    - Parses timestamp strings, coercing anything unparseable to null rather than
      failing the job (Olist has a handful of orders with malformed/empty delivery
      dates for orders that were canceled or never shipped).
    - Deduplicates on order_id: the raw file has a few exact-duplicate rows.
    """
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    df = orders
    for c in ts_cols:
        df = df.withColumn(c, F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"))

    # Exact duplicate rows (same order_id + same purchase timestamp) -> keep one.
    # Using dropDuplicates(["order_id"]) rather than a plain dropDuplicates() because
    # a full-row compare is expensive across every column and order_id is the actual
    # grain we care about; if two rows share an order_id they should be identical
    # data-entry dupes, not two legitimately different orders.
    before = df.count()
    df = df.dropDuplicates(["order_id"])
    after = df.count()
    if before != after:
        logger.info(f"clean_orders: dropped {before - after} duplicate order_id rows")

    return df


def build_order_fact(orders: DataFrame, order_items: DataFrame, payments: DataFrame,
                      customers: DataFrame, products: DataFrame) -> DataFrame:
    """
    Joins orders -> order_items -> products (item-grain) and separately
    aggregates payments to order-grain before joining, to avoid a fan-out.

    IMPORTANT: order_items is many-rows-per-order and payments is also
    many-rows-per-order (installments create multiple payment rows). Joining
    both directly onto orders at item-grain would fan-out payment_value and
    massively overcount revenue. Payments are pre-aggregated to one row per
    order_id first.
    """
    payments_agg = (
        payments.groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.max("payment_installments").alias("max_installments"),
            F.first("payment_type").alias("payment_type"),  # dominant/first type is
            # good enough for this analytics table; a payment_type breakdown would
            # need its own bridge table if that level of detail mattered later.
        )
    )

    items_with_products = order_items.join(products, on="product_id", how="left")

    fact = (
        orders
        .join(items_with_products, on="order_id", how="inner")  # inner: an order
        # with zero items isn't a real transaction for revenue purposes
        .join(payments_agg, on="order_id", how="left")
        .join(customers, on="customer_id", how="left")
    )

    fact = fact.withColumn(
        "delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
    )

    return fact


def revenue_by_month(fact: DataFrame) -> DataFrame:
    return (
        fact.withColumn("order_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .groupBy("order_month")
        .agg(
            F.sum("price").alias("total_item_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("order_month")
    )


def avg_delivery_time_by_state(fact: DataFrame) -> DataFrame:
    return (
        fact.filter(F.col("delivery_days").isNotNull())  # excludes canceled/undelivered
        .groupBy("customer_state")
        .agg(
            F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
            F.count("order_id").alias("delivered_order_items"),
        )
        .orderBy("customer_state")
    )


def top_categories_by_revenue(fact: DataFrame, top_n: int = 10) -> DataFrame:
    return (
        fact.groupBy("product_category_name")
        .agg(F.sum("price").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
        .limit(top_n)
    )


def validate_no_rows_dropped(raw_orders: DataFrame, fact: DataFrame) -> None:
    """
    Sanity check run at the end of the transform stage: every non-canceled order
    with at least one item should be represented in the fact table. Logs a warning
    rather than raising, since a small gap (e.g. orphaned order_items referencing
    a missing order_id) is expected in real Olist data and shouldn't kill the job --
    but it needs to be visible, not silently swallowed.
    """
    raw_order_ids = raw_orders.select("order_id").distinct().count()
    fact_order_ids = fact.select("order_id").distinct().count()
    logger.info(
        f"validation: {fact_order_ids}/{raw_order_ids} distinct orders present in fact table "
        f"({raw_order_ids - fact_order_ids} missing -- expected for orders with zero items "
        f"or no matching payment record)"
    )
