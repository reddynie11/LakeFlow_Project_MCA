import dlt

customer_rules = {
    "rule_1" : "customer_id IS NOT NULL",
    "rule_2" : "customer_name IS NOT NULL"
}

@dlt.table(name="customers_bronze")
@dlt.expect_all(customer_rules)
def customers_bronze():
    return spark.readStream.table("lakeflow_catalog.source.customers")
