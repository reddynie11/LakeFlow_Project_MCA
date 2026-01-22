import dlt

product_rules = {
    "rule_1" : "product_id is NOT NULL",
    "rule_2" : "price >= 0"
}
# Ingesting products into Bronze layer
@dlt.table(
    name="products_raw"
)
@dlt.expect_all(product_rules)
def products_bronze():
    return spark.readStream.table("lakeflow_catalog.source.products")
