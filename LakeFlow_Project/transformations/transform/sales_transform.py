import dlt
from pyspark.sql.functions import col


dlt.create_streaming_table(
    name="sales_clean"
)



@dlt.view(
    name="sales_raw_stg"
)
def sales_enriched_view():
    df = spark.readStream.table(
        "lakeflow_catalog.sales_app.sales_raw"
    )
    df = df.withColumn(
        "total_amount",
        col("quantity") * col("amount")
    )
    return df



dlt.create_auto_cdc_flow(
    target="sales_clean",
    source="sales_raw_stg",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=False
)
