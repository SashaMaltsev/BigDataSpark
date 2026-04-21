import base64
import json
import urllib.request

from pyspark.sql import SparkSession, Window, functions as F


PG_URL = "jdbc:postgresql://postgres:5432/lab2"
PG_PROPS = {
    "user": "lab",
    "password": "lab",
    "driver": "org.postgresql.Driver",
}

CH_HTTP_URL = "http://clickhouse:8123/"
CH_USER = "lab"
CH_PASSWORD = "lab"
CH_DB = "lab2"


def clickhouse_query(sql):
    auth = base64.b64encode(f"{CH_USER}:{CH_PASSWORD}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        f"{CH_HTTP_URL}?database={CH_DB}",
        data=sql.encode("utf-8"),
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()


def read_pg(spark, table_name):
    return spark.read.jdbc(PG_URL, f"public.{table_name}", properties=PG_PROPS)


def insert_json_each_row(df, table_name):
    rows = df.collect()
    print(f"Writing {table_name}: {len(rows)} rows")
    if not rows:
        return

    lines = []
    for row in rows:
        lines.append(json.dumps(row.asDict(), ensure_ascii=False, default=str))

    clickhouse_query(f"INSERT INTO {CH_DB}.{table_name} FORMAT JSONEachRow\n" + "\n".join(lines))


def clean_strings(df, columns):
    result = df
    for column in columns:
        result = result.withColumn(column, F.coalesce(F.col(column), F.lit("")))
    return result


spark = (
    SparkSession.builder
    .appName("lab2-job2-reports-clickhouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

fact_sales = read_pg(spark, "fact_sales")
dim_products = read_pg(spark, "dim_products")
dim_customers = read_pg(spark, "dim_customers")
dim_dates = read_pg(spark, "dim_dates")
dim_stores = read_pg(spark, "dim_stores")
dim_suppliers = read_pg(spark, "dim_suppliers")

clickhouse_query(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")

create_tables = [
    """
    CREATE TABLE lab2.report_product_sales (
        product_id Int32,
        product_name String,
        product_category String,
        total_quantity Int64,
        total_revenue Float64,
        avg_rating Float64,
        total_reviews Int64,
        sales_rank Int64,
        category_revenue Float64
    ) ENGINE = MergeTree ORDER BY product_id
    """,
    """
    CREATE TABLE lab2.report_customer_sales (
        customer_id Int32,
        customer_name String,
        customer_country String,
        total_spent Float64,
        orders_count Int64,
        avg_check Float64,
        customer_rank Int64,
        customers_in_country Int64
    ) ENGINE = MergeTree ORDER BY customer_id
    """,
    """
    CREATE TABLE lab2.report_time_sales (
        sale_year Int32,
        sale_month Int32,
        orders_count Int64,
        total_quantity Int64,
        total_revenue Float64,
        avg_order_amount Float64,
        previous_month_revenue Float64,
        revenue_diff Float64
    ) ENGINE = MergeTree ORDER BY (sale_year, sale_month)
    """,
    """
    CREATE TABLE lab2.report_store_sales (
        store_id String,
        store_name String,
        store_city String,
        store_country String,
        orders_count Int64,
        total_revenue Float64,
        avg_check Float64,
        store_rank Int64,
        city_revenue Float64,
        country_revenue Float64
    ) ENGINE = MergeTree ORDER BY store_id
    """,
    """
    CREATE TABLE lab2.report_supplier_sales (
        supplier_id String,
        supplier_name String,
        supplier_city String,
        supplier_country String,
        total_quantity Int64,
        total_revenue Float64,
        avg_product_price Float64,
        supplier_rank Int64,
        country_revenue Float64
    ) ENGINE = MergeTree ORDER BY supplier_id
    """,
    """
    CREATE TABLE lab2.report_product_quality (
        product_id Int32,
        product_name String,
        product_category String,
        avg_rating Float64,
        total_reviews Int64,
        total_quantity Int64,
        total_revenue Float64,
        high_rating_rank Int64,
        low_rating_rank Int64,
        reviews_rank Int64,
        rating_sales_correlation Float64
    ) ENGINE = MergeTree ORDER BY product_id
    """,
]

for table_name in [
    "report_product_sales",
    "report_customer_sales",
    "report_time_sales",
    "report_store_sales",
    "report_supplier_sales",
    "report_product_quality",
]:
    clickhouse_query(f"DROP TABLE IF EXISTS {CH_DB}.{table_name}")

for sql in create_tables:
    clickhouse_query(sql)

product_sales = (
    fact_sales.join(dim_products, "product_id", "left")
    .groupBy("product_id", "product_name", "product_category")
    .agg(
        F.sum("sale_quantity").cast("long").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_rating").alias("avg_rating"),
        F.max("product_reviews").cast("long").alias("total_reviews"),
    )
    .withColumn("sales_rank", F.dense_rank().over(Window.orderBy(F.desc("total_quantity"))).cast("long"))
    .withColumn("category_revenue", F.sum("total_revenue").over(Window.partitionBy("product_category")))
)
product_sales = clean_strings(product_sales, ["product_name", "product_category"])
product_sales = product_sales.select(
    "product_id",
    "product_name",
    "product_category",
    F.coalesce("total_quantity", F.lit(0)).cast("long").alias("total_quantity"),
    F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
    F.coalesce("avg_rating", F.lit(0.0)).alias("avg_rating"),
    F.coalesce("total_reviews", F.lit(0)).cast("long").alias("total_reviews"),
    "sales_rank",
    F.coalesce("category_revenue", F.lit(0.0)).alias("category_revenue"),
)

customer_sales = (
    fact_sales.join(dim_customers, "customer_id", "left")
    .withColumn("customer_name", F.concat_ws(" ", "customer_first_name", "customer_last_name"))
    .groupBy("customer_id", "customer_name", "customer_country")
    .agg(
        F.sum("sale_total_price").alias("total_spent"),
        F.count("*").cast("long").alias("orders_count"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    .withColumn("customer_rank", F.dense_rank().over(Window.orderBy(F.desc("total_spent"))).cast("long"))
    .withColumn("customers_in_country", F.count("*").over(Window.partitionBy("customer_country")).cast("long"))
)
customer_sales = clean_strings(customer_sales, ["customer_name", "customer_country"])
customer_sales = customer_sales.select(
    "customer_id",
    "customer_name",
    "customer_country",
    F.coalesce("total_spent", F.lit(0.0)).alias("total_spent"),
    "orders_count",
    F.coalesce("avg_check", F.lit(0.0)).alias("avg_check"),
    "customer_rank",
    "customers_in_country",
)

time_sales = (
    fact_sales.join(dim_dates, "date_id", "left")
    .groupBy("sale_year", "sale_month")
    .agg(
        F.count("*").cast("long").alias("orders_count"),
        F.sum("sale_quantity").cast("long").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("sale_total_price").alias("avg_order_amount"),
    )
)
time_window = Window.orderBy("sale_year", "sale_month")
time_sales = (
    time_sales
    .withColumn("previous_month_revenue", F.lag("total_revenue").over(time_window))
    .withColumn("previous_month_revenue", F.coalesce("previous_month_revenue", F.lit(0.0)))
    .withColumn("revenue_diff", F.col("total_revenue") - F.col("previous_month_revenue"))
    .select(
        F.coalesce("sale_year", F.lit(0)).cast("int").alias("sale_year"),
        F.coalesce("sale_month", F.lit(0)).cast("int").alias("sale_month"),
        "orders_count",
        F.coalesce("total_quantity", F.lit(0)).cast("long").alias("total_quantity"),
        F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
        F.coalesce("avg_order_amount", F.lit(0.0)).alias("avg_order_amount"),
        "previous_month_revenue",
        F.coalesce("revenue_diff", F.lit(0.0)).alias("revenue_diff"),
    )
)

store_sales = (
    fact_sales.join(dim_stores, "store_id", "left")
    .groupBy("store_id", "store_name", "store_city", "store_country")
    .agg(
        F.count("*").cast("long").alias("orders_count"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    .withColumn("store_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))).cast("long"))
    .withColumn("city_revenue", F.sum("total_revenue").over(Window.partitionBy("store_city")))
    .withColumn("country_revenue", F.sum("total_revenue").over(Window.partitionBy("store_country")))
)
store_sales = clean_strings(store_sales, ["store_id", "store_name", "store_city", "store_country"])
store_sales = store_sales.select(
    "store_id",
    "store_name",
    "store_city",
    "store_country",
    "orders_count",
    F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
    F.coalesce("avg_check", F.lit(0.0)).alias("avg_check"),
    "store_rank",
    F.coalesce("city_revenue", F.lit(0.0)).alias("city_revenue"),
    F.coalesce("country_revenue", F.lit(0.0)).alias("country_revenue"),
)

supplier_sales = (
    fact_sales.join(dim_suppliers, "supplier_id", "left")
    .groupBy("supplier_id", "supplier_name", "supplier_city", "supplier_country")
    .agg(
        F.sum("sale_quantity").cast("long").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_price").alias("avg_product_price"),
    )
    .withColumn("supplier_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))).cast("long"))
    .withColumn("country_revenue", F.sum("total_revenue").over(Window.partitionBy("supplier_country")))
)
supplier_sales = clean_strings(supplier_sales, ["supplier_id", "supplier_name", "supplier_city", "supplier_country"])
supplier_sales = supplier_sales.select(
    "supplier_id",
    "supplier_name",
    "supplier_city",
    "supplier_country",
    F.coalesce("total_quantity", F.lit(0)).cast("long").alias("total_quantity"),
    F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
    F.coalesce("avg_product_price", F.lit(0.0)).alias("avg_product_price"),
    "supplier_rank",
    F.coalesce("country_revenue", F.lit(0.0)).alias("country_revenue"),
)

product_quality = (
    fact_sales.join(dim_products, "product_id", "left")
    .groupBy("product_id", "product_name", "product_category")
    .agg(
        F.avg("product_rating").alias("avg_rating"),
        F.max("product_reviews").cast("long").alias("total_reviews"),
        F.sum("sale_quantity").cast("long").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
    )
    .withColumn("high_rating_rank", F.dense_rank().over(Window.orderBy(F.desc("avg_rating"))).cast("long"))
    .withColumn("low_rating_rank", F.dense_rank().over(Window.orderBy(F.asc("avg_rating"))).cast("long"))
    .withColumn("reviews_rank", F.dense_rank().over(Window.orderBy(F.desc("total_reviews"))).cast("long"))
    .withColumn("rating_sales_correlation", F.corr("avg_rating", "total_quantity").over(Window.partitionBy()))
)
product_quality = clean_strings(product_quality, ["product_name", "product_category"])
product_quality = product_quality.select(
    "product_id",
    "product_name",
    "product_category",
    F.coalesce("avg_rating", F.lit(0.0)).alias("avg_rating"),
    F.coalesce("total_reviews", F.lit(0)).cast("long").alias("total_reviews"),
    F.coalesce("total_quantity", F.lit(0)).cast("long").alias("total_quantity"),
    F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
    "high_rating_rank",
    "low_rating_rank",
    "reviews_rank",
    F.coalesce("rating_sales_correlation", F.lit(0.0)).alias("rating_sales_correlation"),
)

insert_json_each_row(product_sales, "report_product_sales")
insert_json_each_row(customer_sales, "report_customer_sales")
insert_json_each_row(time_sales, "report_time_sales")
insert_json_each_row(store_sales, "report_store_sales")
insert_json_each_row(supplier_sales, "report_supplier_sales")
insert_json_each_row(product_quality, "report_product_quality")

spark.stop()
