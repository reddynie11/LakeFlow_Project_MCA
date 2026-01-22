import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

dlt.create_streaming_table(
    name="products_clean"
)

@dlt.view(
    name="product_raw_stg"
)
def products_enriched_view():
    df = spark.readStream.table(
        "lakeflow_catalog.sales_app.products_raw"
    )
    df = df.withColumn(
        "price",
        col("price").cast(IntegerType())
    )
    return df

dlt.create_auto_cdc_flow(
    target="products_clean",
    source="product_raw_stg",
    keys=["product_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1,
    ignore_null_updates=False
)
