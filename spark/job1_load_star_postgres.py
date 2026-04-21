from pyspark.sql import SparkSession, functions as F


PG_URL = "jdbc:postgresql://postgres:5432/lab2"
PG_PROPS = {
    "user": "lab",
    "password": "lab",
    "driver": "org.postgresql.Driver",
}


def write_postgres(df, table_name):
    print(f"Writing {table_name}: {df.count()} rows")
    (
        df.write
        .mode("overwrite")
        .jdbc(PG_URL, table_name, properties=PG_PROPS)
    )


def first_value(column_name):
    return F.first(F.col(column_name), ignorenulls=True).alias(column_name)


spark = (
    SparkSession.builder
    .appName("lab2-job1-star-postgres")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

raw = spark.read.jdbc(PG_URL, "public.mock_data", properties=PG_PROPS)

sales = (
    raw
    .withColumn("source_sale_id", F.col("id").cast("int"))
    .withColumn("customer_id", F.col("sale_customer_id").cast("int"))
    .withColumn("seller_id", F.col("sale_seller_id").cast("int"))
    .withColumn("product_id", F.col("sale_product_id").cast("int"))
    .withColumn("product_price", F.col("product_price").cast("double"))
    .withColumn("product_quantity", F.col("product_quantity").cast("int"))
    .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
    .withColumn("sale_total_price", F.col("sale_total_price").cast("double"))
    .withColumn("product_weight", F.col("product_weight").cast("double"))
    .withColumn("product_rating", F.col("product_rating").cast("double"))
    .withColumn("product_reviews", F.col("product_reviews").cast("int"))
    .withColumn("customer_age", F.col("customer_age").cast("int"))
    .withColumn("sale_date", F.to_date("sale_date", "M/d/yyyy"))
    .withColumn("product_release_date", F.to_date("product_release_date", "M/d/yyyy"))
    .withColumn("product_expiry_date", F.to_date("product_expiry_date", "M/d/yyyy"))
    .withColumn(
        "store_id",
        F.sha2(F.coalesce(F.col("store_name"), F.lit("")), 256),
    )
    .withColumn(
        "supplier_id",
        F.sha2(F.coalesce(F.col("supplier_name"), F.lit("")), 256),
    )
    .withColumn("date_id", F.date_format("sale_date", "yyyyMMdd").cast("int"))
)

dim_customers = (
    sales
    .groupBy("customer_id")
    .agg(
        first_value("customer_first_name"),
        first_value("customer_last_name"),
        first_value("customer_age"),
        first_value("customer_email"),
        first_value("customer_country"),
        first_value("customer_postal_code"),
        first_value("customer_pet_type"),
        first_value("customer_pet_name"),
        first_value("customer_pet_breed"),
    )
)

dim_sellers = (
    sales
    .groupBy("seller_id")
    .agg(
        first_value("seller_first_name"),
        first_value("seller_last_name"),
        first_value("seller_email"),
        first_value("seller_country"),
        first_value("seller_postal_code"),
    )
)

dim_products = (
    sales
    .groupBy("product_id")
    .agg(
        first_value("product_name"),
        first_value("product_category"),
        first_value("pet_category"),
        first_value("product_weight"),
        first_value("product_color"),
        first_value("product_size"),
        first_value("product_brand"),
        first_value("product_material"),
        first_value("product_description"),
        first_value("product_rating"),
        first_value("product_reviews"),
        first_value("product_release_date"),
        first_value("product_expiry_date"),
    )
)

dim_stores = (
    sales
    .groupBy("store_id")
    .agg(
        first_value("store_name"),
        first_value("store_location"),
        first_value("store_city"),
        first_value("store_state"),
        first_value("store_country"),
        first_value("store_phone"),
        first_value("store_email"),
    )
)

dim_suppliers = (
    sales
    .groupBy("supplier_id")
    .agg(
        first_value("supplier_name"),
        first_value("supplier_contact"),
        first_value("supplier_email"),
        first_value("supplier_phone"),
        first_value("supplier_address"),
        first_value("supplier_city"),
        first_value("supplier_country"),
    )
)

dim_dates = (
    sales
    .select("date_id", "sale_date")
    .dropna(subset=["date_id"])
    .dropDuplicates(["date_id"])
    .withColumn("sale_year", F.year("sale_date"))
    .withColumn("sale_month", F.month("sale_date"))
    .withColumn("sale_day", F.dayofmonth("sale_date"))
)

fact_sales = (
    sales
    .select(
        "source_sale_id",
        "customer_id",
        "seller_id",
        "product_id",
        "store_id",
        "supplier_id",
        "date_id",
        "product_price",
        "product_quantity",
        "sale_quantity",
        "sale_total_price",
    )
)

write_postgres(dim_customers, "public.dim_customers")
write_postgres(dim_sellers, "public.dim_sellers")
write_postgres(dim_products, "public.dim_products")
write_postgres(dim_stores, "public.dim_stores")
write_postgres(dim_suppliers, "public.dim_suppliers")
write_postgres(dim_dates, "public.dim_dates")
write_postgres(fact_sales, "public.fact_sales")

spark.stop()
