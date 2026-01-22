import dlt
from pyspark.sql.functions import col, upper

# ---------------------------------------------------
# Step 1: Create a Silver TABLE (base streaming table)
# ---------------------------------------------------

dlt.create_streaming_table(
    name="customers_silver"
)

# ---------------------------------------------------
# Step 2: Create a Silver VIEW (transformation logic)
# ---------------------------------------------------

@dlt.view(
    name="customers_silver_view"
)
def customers_silver_view():
    df = spark.readStream.table(
        "lakeflow_catalog.bronze.customers_bronze"
    )
    df = df.withColumn(
        "customer_name",
        upper(col("customer_name"))
    )
    return df

# ---------------------------------------------------
# Step 3: Apply CDC logic into Silver TABLE
# ---------------------------------------------------

dlt.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_silver_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1,
    ignore_null_updates=False
)
