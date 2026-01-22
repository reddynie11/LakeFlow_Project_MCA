import dlt

# Ingesting products into Bronze layer
@dlt.table(
    name="products_bronze"
)
def products_bronze():
    return spark.readStream.table("lakeflow_catalog.source.products")
