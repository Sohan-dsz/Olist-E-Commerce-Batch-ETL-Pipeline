"""
Load stage: pushes the aggregated (small) tables to BigQuery using the
google-cloud-bigquery Python client directly, via pandas, rather than the
Spark BigQuery connector.

Why not the spark-bigquery-connector: that connector shines when you're writing
huge fact tables and want distributed writes straight from executors. Here the
three output tables (monthly revenue, delivery-by-state, top categories) are all
tiny -- a few dozen to a few hundred rows -- so collecting to the driver and using
the plain Python client avoids pulling in the connector jar/GCS staging bucket
requirement for no real benefit. If this pipeline ever writes the full order-grain
fact table (millions of rows) to BigQuery, switch to the Spark connector instead.
"""
import logging
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pyspark.sql import DataFrame

logger = logging.getLogger("olist_pipeline.load")


def get_or_create_dataset(client: bigquery.Client, project: str, dataset_id: str, location: str = "US"):
    dataset_ref = f"{project}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists")
        
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_ref}")


def load_table(client: bigquery.Client, spark_df: DataFrame, project: str,
                dataset_id: str, table_name: str, write_disposition: str = "WRITE_TRUNCATE"):
    """
    Collects a (small, aggregated) Spark DataFrame to the driver as pandas,
    then loads it via the BigQuery client's load_table_from_dataframe, which
    handles schema inference + creation for us.
    """
    pdf: pd.DataFrame = spark_df.toPandas()
    table_ref = f"{project}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,  # WRITE_TRUNCATE = idempotent reruns;
        # this pipeline is a full batch recompute each run, not an incremental
        # append, so truncate-and-reload is the correct semantics here.
        autodetect=True,
    )
    job = client.load_table_from_dataframe(pdf, table_ref, job_config=job_config)
    job.result()  # blocks until the load finishes
    logger.info(f"Loaded {len(pdf)} rows into {table_ref}")


def load_all(spark_tables: dict, project: str, dataset_id: str, location: str = "US"):
    client = bigquery.Client(project=project)
    get_or_create_dataset(client, project, dataset_id, location)
    for table_name, df in spark_tables.items():
        load_table(client, df, project, dataset_id, table_name)
