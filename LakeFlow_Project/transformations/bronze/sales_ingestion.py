import dlt

# Sales Expectations
sales_rules = {
    "rule_1": "sales_id IS NOT NULL"
}

# Create empty bronze streaming table for consolidated sales
dlt.create_streaming_table(
    name="sales_bronze",
    expect_all_or_drop=sales_rules
)

# Ingest East region sales into bronze
@dlt.append_flow(target="sales_bronze")
def sales_east_bronze():
    return spark.readStream.table("lakeflow_catalog.source.sales_east")

# Ingest West region sales into bronze
@dlt.append_flow(target="sales_bronze")
def sales_west_bronze():
    return spark.readStream.table("lakeflow_catalog.source.sales_west")
