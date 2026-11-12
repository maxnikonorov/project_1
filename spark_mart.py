from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime



spark = SparkSession.builder \
    .appName("ClickHouseToPySpark") \
    .master("local[*]") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

jdbc_options = {
    "url": "jdbc:clickhouse://clickhouse:8123/clean_dwh?compress=0",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": ""
}

df_purchases = spark.read.format("jdbc").options(**jdbc_options).option("query", "SELECT * FROM clean_dwh.purchases").load()
df_customers = spark.read.format("jdbc").options(**jdbc_options).option("query", "SELECT * FROM clean_dwh.customers FINAL").load()
df_products  = spark.read.format("jdbc").options(**jdbc_options).option("query", "SELECT * FROM clean_dwh.products FINAL").load()
df_stores    = spark.read.format("jdbc").options(**jdbc_options).option("query", "SELECT * FROM clean_dwh.stores FINAL").load()


result_df = (
    df_purchases
    .join(df_customers, on="customer_id", how="left")
    .join(df_stores, on="store_id", how="left")
    .join(df_products, on="product_id", how="left")
)

mark = result_df.groupBy("customer_id").agg(
    # Покупал молочные продукты за последние 30 дней
    F.max(
        F.when(
            F.col("category").ilike("%молочные продукты%")
            & (F.datediff(F.current_date(), F.col("purchase_datetime")) <= 30),
            1,
        ).otherwise(0)
    ).alias("bought_milk_last_30d"),
    # Покупал фрукты и ягоды за последние 14 дней
    F.max(
        F.when(
            F.col("category").ilike("%фрукты%")
            & (F.datediff(F.current_date(), F.col("purchase_datetime")) <= 14),
            1,
        ).otherwise(0)
    ).alias("bought_fruits_last_14d"),
    # Не покупал овощи и зелень за последние 14 дней
    F.min(
        F.when(
            F.col("category").ilike("%овощи%")
            & (F.datediff(F.current_date(), F.col("purchase_datetime")) <= 14),
            0,
        ).otherwise(1)
    ).alias("not_bought_veggies_14d"),
    # Делал более 2 покупок за последние 30 дней
    F.when(
        F.countDistinct(
            F.when(
                F.datediff(F.current_date(), F.col("purchase_datetime")) <= 30,
                F.col("purchase_id")
            )
        ) > 2,
        1
    ).otherwise(0).alias("more_than_2_purchases_30d"),
    # Не покупал 14–30 дней (ушедший клиент?)
    F.when(
        F.datediff(F.current_date(), F.max("purchase_datetime")).between(14, 30),
        1
    ).otherwise(0).alias("inactive_14_30"),
    # Покупатель зарегистрировался менее 30 дней назад
    F.when(
        F.date_diff(F.current_date(), F.max("registration_date")) < 30,
        1
    ).otherwise(0).alias("new_customer"),
    # Пользовался доставкой хотя бы раз
    F.max(
        F.when(
            F.col("is_delivery") == 1,
            1,
        ).otherwise(0)
    ).alias("delivery_user"),
    # Средняя корзина > 1000₽
    F.when(
        (F.sum(F.col("price") * F.col("quantity")) / F.countDistinct("purchase_id")) > 1000,
        1
    ).otherwise(0).alias("bulk_buyer"),
    # Средняя корзина < 200₽
    F.when(
        (F.sum(F.col("price") * F.col("quantity")) / F.countDistinct("purchase_id")) < 200,
        1
    ).otherwise(0).alias("low_cost_buyer"),
    # Покупал хлеб/выпечку хотя бы раз
    F.max(
        F.when(
            F.col("category").ilike("%хлеб%"),
            1,
        ).otherwise(0)
    ).alias("buys_bakery"),
    # Лояльный клиент (≥3 покупки)
    F.when(
        F.countDistinct("purchase_id") >= 3,
        1
    ).otherwise(0).alias("loyal_customer"),
    # Делал покупки в разных городах
    F.when(
        F.countDistinct(F.col("city")) > 1,
        1
    ).otherwise(0).alias("multicity_buyer"),
    # Не совершал ни одной покупки (только регистрация)
    F.when(
        F.count("purchase_id") == 0,
        1
    ).otherwise(0).alias("no_purchases"),
    # Не купил ни одного мясного продукта за 90 дней
    F.min(
        F.when(
            F.col("category").ilike("%мяс%")
            & (F.datediff(F.current_date(), F.col("purchase_datetime")) <= 90),
            0,
        ).otherwise(1)
    ).alias("vegetarian_profile")
)

mark.show(1, truncate=False)

run_date = datetime.now().strftime("%Y-%m-%d")

mark.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .option("sep", ",") \
    .csv(f"/opt/airflow/project_1/csv_files/csv_analytic_result_{run_date}")

spark.stop()
