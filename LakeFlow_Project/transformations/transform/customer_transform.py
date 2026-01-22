import dlt
from pyspark.sql.functions import col, upper


dlt.create_streaming_table(
    name="customers_clean"
)


@dlt.view(
    name="cust_raw_stg_view"
)
def customers_silver_view():
    df = spark.readStream.table(
        "lakeflow_catalog.bronze.customers_raw"
    )
    df = df.withColumn(
        "customer_name",
        upper(col("customer_name"))
    )
    return df


dlt.create_auto_cdc_flow(
    target="customers_clean",
    source="cust_raw_stg_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1,
    ignore_null_updates=False
)
