"""
End-to-end orchestrator: Extract -> Transform -> Load.

Usage:
    python src/pipeline.py --data-dir data/raw --out-dir data/output
    python src/pipeline.py --data-dir data/raw --load-to-bq --bq-project my-gcp-project --bq-dataset olist_analytics

Run without --load-to-bq to just extract+transform and write results locally as
parquet/CSV under --out-dir -- useful for iterating without needing GCP credentials
on hand every time.
"""
import argparse
import logging
import sys

from extract import get_spark, extract_all
from transform import (
    clean_orders, build_order_fact, revenue_by_month,
    avg_delivery_time_by_state, top_categories_by_revenue,
    validate_no_rows_dropped,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("olist_pipeline")


def run(data_dir: str, out_dir: str, load_to_bq: bool, bq_project: str, bq_dataset: str):
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("EXTRACT: reading raw CSVs from %s", data_dir)
    raw = extract_all(spark, data_dir)

    logger.info("TRANSFORM: cleaning + joining")
    orders_clean = clean_orders(raw["orders"])
    fact = build_order_fact(
        orders_clean, raw["order_items"], raw["payments"],
        raw["customers"], raw["products"],
    ).cache()  # cached because all three aggregations below re-read this same
    # DataFrame; without .cache() Spark would redo the full extract+join chain
    # three times (lazy evaluation means nothing has actually run yet at this point).

    validate_no_rows_dropped(raw["orders"], fact)

    monthly_revenue = revenue_by_month(fact)
    delivery_by_state = avg_delivery_time_by_state(fact)
    top_categories = top_categories_by_revenue(fact)

    results = {
        "monthly_revenue": monthly_revenue,
        "avg_delivery_by_state": delivery_by_state,
        "top_categories_by_revenue": top_categories,
    }

    logger.info("Sample output -- monthly revenue:")
    monthly_revenue.show(5, truncate=False)
    logger.info("Sample output -- avg delivery by state:")
    delivery_by_state.show(5, truncate=False)
    logger.info("Sample output -- top categories:")
    top_categories.show(5, truncate=False)

    if load_to_bq:
        logger.info("LOAD: writing tables to BigQuery %s.%s", bq_project, bq_dataset)
        from load import load_all
        load_all(results, project=bq_project, dataset_id=bq_dataset)
    else:
        logger.info("LOAD: --load-to-bq not set, writing results locally to %s instead", out_dir)
        for name, df in results.items():
            (
                df.coalesce(1)  # single output file -- these tables are tiny
                .write.mode("overwrite")
                .option("header", True)
                .csv(f"{out_dir}/{name}")
            )

    fact.unpersist()
    spark.stop()
    logger.info("Pipeline complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--out-dir", default="data/output")
    parser.add_argument("--load-to-bq", action="store_true")
    parser.add_argument("--bq-project", default=None)
    parser.add_argument("--bq-dataset", default="olist_analytics")
    args = parser.parse_args()

    if args.load_to_bq and not args.bq_project:
        parser.error("--bq-project is required when --load-to-bq is set")

    run(args.data_dir, args.out_dir, args.load_to_bq, args.bq_project, args.bq_dataset)
