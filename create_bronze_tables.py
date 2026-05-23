from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# customers
df_customers = spark.read.option("header", True).csv(
    "Files/bronze_raw/customers.csv"
)
df_customers = df_customers.withColumn("LoadDate", current_timestamp())
df_customers.write.format("delta").mode("overwrite").saveAsTable("bronze.customers")
print(f"bronze.customers: {df_customers.count()} rows")

# cards
df_cards = spark.read.option("header", True).csv(
    "Files/bronze_raw/cards.csv"
)
df_cards = df_cards.withColumn("LoadDate", current_timestamp())
df_cards.write.format("delta").mode("overwrite").saveAsTable("bronze.cards")
print(f"bronze.cards: {df_cards.count()} rows")

# merchants
df_merchants = spark.read.option("header", True).csv(
    "Files/bronze_raw/merchants.csv"
)
df_merchants = df_merchants.withColumn("LoadDate", current_timestamp())
df_merchants.write.format("delta").mode("overwrite").saveAsTable("bronze.merchants")
print(f"bronze.merchants: {df_merchants.count()} rows")

# transactions
df_transactions = spark.read.option("header", True).csv(
    "Files/bronze_raw/transactions.csv"
)
df_transactions = df_transactions.withColumn("LoadDate", current_timestamp())
df_transactions.write.format("delta").mode("overwrite").saveAsTable("bronze.transactions")
print(f"bronze.transactions: {df_transactions.count()} rows")

print("\nAll Bronze tables created")


