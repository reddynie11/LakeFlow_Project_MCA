import dlt

@dlt.table(name="customers_bronze")
def customers_bronze():
    return spark.readStream.table("lakeflow_catalog.source.customers")
