from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Создаем сессию Spark
spark = SparkSession.builder \
    .appName("Products and Categories") \
    .getOrCreate()

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

#DataFrame для категорий
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

#Df связей
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

# join для пары имя - категория
product_category_join_df = product_category_df \
    .join(products_df, "product_id") \
    .join(categories_df, "category_id") \
    .select("product_name", "category_name")

# продукт без категории
products_with_no_category_df = products_df \
    .join(product_category_df, "product_id", "left_anti") \
    .select("product_name")

product_category_join_df.show(truncate=False)
products_with_no_category_df.show(truncate=False)
