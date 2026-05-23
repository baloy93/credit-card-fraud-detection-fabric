from pyspark.sql.functions import (
    col, trim, upper, initcap, to_date, current_date,
    when, year, regexp_replace, lower
)
from pyspark.sql.types import DecimalType, IntegerType, BooleanType

# ── silver.customers ───────────────────────────────────
df_bronze_cust = spark.table("bronze.customers")

df_silver_cust = df_bronze_cust \
    .filter(col("CustomerID").isNotNull()) \
    .dropDuplicates(["CustomerID"]) \
    .withColumn("FullName",    initcap(trim(col("FullName")))) \
    .withColumn("Gender",      upper(trim(col("Gender")))) \
    .withColumn("City",        initcap(trim(col("City")))) \
    .withColumn("Country",     initcap(trim(col("Country")))) \
    .withColumn("Email",       lower(trim(col("Email")))) \
    .withColumn("DateOfBirth", to_date(col("DateOfBirth"), "yyyy-MM-dd")) \
    .withColumn("Age",
        (year(current_date()) - year(col("DateOfBirth"))).cast(IntegerType())
    ) \
    .withColumn("AgeGroup",
        when(col("Age") < 25, "Youth")
        .when(col("Age") <= 45, "Adult")
        .otherwise("Senior")
    ) \
    .withColumn("IsValidEmail",
        col("Email").contains("@")
    ) \
    .filter(col("Age").between(18, 100)) \
    .filter(col("FullName").isNotNull())

df_silver_cust.write.format("delta").mode("overwrite") \
    .saveAsTable("silver.customers")
print(f"silver.customers: {df_silver_cust.count()} rows")


# ── silver.cards ───────────────────────────────────────
df_bronze_cards = spark.table("bronze.cards")

df_silver_cards = df_bronze_cards \
    .filter(col("CardID").isNotNull()) \
    .filter(col("CustomerID").isNotNull()) \
    .dropDuplicates(["CardID"]) \
    .withColumn("CardType",   upper(trim(col("CardType")))) \
    .withColumn("CardStatus", upper(trim(col("CardStatus")))) \
    .withColumn("CreditLimit",
        regexp_replace(col("CreditLimit"), ",", "")
        .cast(DecimalType(18, 2))
    ) \
    .withColumn("ExpiryDate", to_date(col("ExpiryDate"), "yyyy-MM-dd")) \
    .withColumn("CardTier",
        when(col("CreditLimit") >= 100000, "Platinum")
        .when(col("CreditLimit") >= 50000, "Gold")
        .otherwise("Silver")
    ) \
    .withColumn("IsExpired",
        col("ExpiryDate") < current_date()
    ) \
    .withColumn("AvailableCredit",
        (col("CreditLimit") * 0.8).cast(DecimalType(18, 2))
    ) \
    .filter(col("CreditLimit") > 0)

df_silver_cards.write.format("delta").mode("overwrite") \
    .saveAsTable("silver.cards")
print(f"silver.cards: {df_silver_cards.count()} rows")


# ── silver.merchants ───────────────────────────────────
df_bronze_merch = spark.table("bronze.merchants")

df_silver_merch = df_bronze_merch \
    .filter(col("MerchantID").isNotNull()) \
    .dropDuplicates(["MerchantID"]) \
    .withColumn("MerchantName",     initcap(trim(col("MerchantName")))) \
    .withColumn("MerchantCategory", upper(trim(col("MerchantCategory")))) \
    .withColumn("City",             initcap(trim(col("City")))) \
    .withColumn("Country",          initcap(trim(col("Country")))) \
    .withColumn("RiskLevel",        upper(trim(col("RiskLevel")))) \
    .withColumn("IsHighRisk",
        col("RiskLevel") == "HIGH"
    ) \
    .withColumn("MerchantSegment",
        when(col("MerchantCategory") == "TRAVEL", "Premium")
        .when(col("MerchantCategory") == "GAMBLING", "Risky")
        .otherwise("Standard")
    )

df_silver_merch.write.format("delta").mode("overwrite") \
    .saveAsTable("silver.merchants")
print(f"silver.merchants: {df_silver_merch.count()} rows")


# ── silver.transactions ────────────────────────────────
from pyspark.sql.functions import (
    hour, date_format, abs as spark_abs
)

df_bronze_tx = spark.table("bronze.transactions")

df_silver_tx = df_bronze_tx \
    .filter(col("TransactionID").isNotNull()) \
    .filter(col("CustomerID").isNotNull()) \
    .filter(col("CardID").isNotNull()) \
    .filter(col("MerchantID").isNotNull()) \
    .dropDuplicates(["TransactionID"]) \
    .withColumn("Amount",
        spark_abs(
            regexp_replace(col("Amount"), ",", "")
            .cast(DecimalType(18, 2))
        )
    ) \
    .filter(col("Amount") > 0) \
    .withColumn("FraudScore",
        col("FraudScore").cast(DecimalType(5, 2))
    ) \
    .withColumn("Currency", upper(trim(col("Currency")))) \
    .withColumn("Channel",  upper(trim(col("Channel")))) \
    .withColumn("TransactionDate",
        to_date(col("TransactionDate"), "yyyy-MM-dd")
    ) \
    .filter(col("TransactionDate") <= current_date()) \
    .withColumn("IsFraud",
        (col("Amount") > 10000) | (col("FraudScore") > 0.80)
    ) \
    .withColumn("SpendCategory",
        when(col("Amount") < 100,  "Low Spend")
        .when(col("Amount") <= 500, "Medium Spend")
        .otherwise("High Spend")
    ) \
    .withColumn("HighValueIndicator",
        col("Amount") >= 5000
    ) \
    .withColumn("TransactionHour",
        hour(col("TransactionDate").cast("timestamp"))
    ) \
    .withColumn("DayName",
        date_format(col("TransactionDate"), "EEEE")
    )

df_silver_tx.write.format("delta").mode("overwrite") \
    .saveAsTable("silver.transactions")
print(f"silver.transactions: {df_silver_tx.count()} rows")


# ── silver.dates ───────────────────────────────────────
from pyspark.sql.functions import (
    dayofmonth, month, year, quarter,
    date_format, dayofweek
)

df_dates = spark.sql("""
    SELECT DISTINCT
        CAST(DATE_FORMAT(TransactionDate, 'yyyyMMdd') AS INT) AS DateKey,
        TransactionDate AS FullDate,
        DAY(TransactionDate)     AS Day,
        MONTH(TransactionDate)   AS Month,
        YEAR(TransactionDate)    AS Year,
        QUARTER(TransactionDate) AS Quarter,
        DATE_FORMAT(TransactionDate, 'EEEE') AS WeekDay
    FROM silver.transactions
    WHERE TransactionDate IS NOT NULL
""")

df_dates = df_dates.withColumn(
    "IsWeekend",
    col("WeekDay").isin("Saturday", "Sunday")
)

df_dates.write.format("delta").mode("overwrite") \
    .saveAsTable("silver.dates")
print(f"silver.dates: {df_dates.count()} rows")

print("\nAll Silver tables loaded")