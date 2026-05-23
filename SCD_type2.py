from pyspark.sql.functions import (
    col, lit, current_date, monotonically_increasing_id,
    when
)
from delta.tables import DeltaTable

# ── Dim_Date ───────────────────────────────────────────
df_dates = spark.sql("""
    SELECT DISTINCT DateKey, FullDate, Day, Month,
                    Year, Quarter, WeekDay, IsWeekend
    FROM silver.dates
    WHERE FullDate IS NOT NULL
""")

existing_keys = spark.sql("SELECT DateKey FROM gold.Dim_Date")
new_dates = df_dates.join(existing_keys, on='DateKey', how='left_anti')
new_dates.write.format("delta").mode("append").saveAsTable("gold.Dim_Date")
print(f"Dim_Date: {new_dates.count()} new dates loaded")


# ── Dim_Customer SCD Type 2 ────────────────────────────
df_silver_cust = spark.sql("""
    SELECT DISTINCT CustomerID, FullName, Gender, City,
                    Country, Email, DateOfBirth,
                    Age, AgeGroup, IsValidEmail
    FROM silver.customers
    WHERE CustomerID IS NOT NULL
""")

dim_cust = DeltaTable.forName(spark, "gold.Dim_Customer")

# expire changed records
dim_cust.alias("t").merge(
    df_silver_cust.alias("s"),
    "t.CustomerID = s.CustomerID AND t.IsCurrent = true"
).whenMatchedUpdate(
    condition="""
        t.City     != s.City     OR
        t.FullName != s.FullName OR
        t.Gender   != s.Gender   OR
        t.Age      != s.Age
    """,
    set={
        "EndDate":   "current_date()",
        "IsCurrent": "false"
    }
).execute()

# insert new and updated records
existing_active = spark.sql(
    "SELECT CustomerID FROM gold.Dim_Customer WHERE IsCurrent = true"
)
new_customers = df_silver_cust.join(
    existing_active, on='CustomerID', how='left_anti'
).withColumn("StartDate", current_date()) \
 .withColumn("EndDate",   lit(None).cast("date")) \
 .withColumn("IsCurrent", lit(True))

max_key = spark.sql(
    "SELECT COALESCE(MAX(CustomerKey), 0) AS mk FROM gold.Dim_Customer"
).collect()[0]['mk']

new_customers = new_customers.withColumn(
    "CustomerKey",
    (monotonically_increasing_id() + max_key + 1).cast("bigint")
)

new_customers.select(
    "CustomerKey", "CustomerID", "FullName", "Gender",
    "City", "Country", "Email", "DateOfBirth",
    "Age", "AgeGroup", "IsValidEmail",
    "StartDate", "EndDate", "IsCurrent"
).write.format("delta").mode("append").saveAsTable("gold.Dim_Customer")
print(f"Dim_Customer: {new_customers.count()} new records loaded")


# ── Dim_Card SCD Type 2 ────────────────────────────────
df_silver_card = spark.sql("""
    SELECT DISTINCT CardID, CustomerID, CardType, CardStatus,
                    ExpiryDate, CreditLimit, CardTier,
                    IsExpired, AvailableCredit
    FROM silver.cards
    WHERE CardID IS NOT NULL AND CustomerID IS NOT NULL
""")

dim_card = DeltaTable.forName(spark, "gold.Dim_Card")

dim_card.alias("t").merge(
    df_silver_card.alias("s"),
    "t.CardID = s.CardID AND t.IsCurrent = true"
).whenMatchedUpdate(
    condition="""
        t.CardType    != s.CardType    OR
        t.CreditLimit != s.CreditLimit OR
        t.CardStatus  != s.CardStatus
    """,
    set={
        "EndDate":   "current_date()",
        "IsCurrent": "false"
    }
).execute()

existing_active_cards = spark.sql(
    "SELECT CardID FROM gold.Dim_Card WHERE IsCurrent = true"
)
new_cards = df_silver_card.join(
    existing_active_cards, on='CardID', how='left_anti'
).withColumn("StartDate", current_date()) \
 .withColumn("EndDate",   lit(None).cast("date")) \
 .withColumn("IsCurrent", lit(True))

max_card_key = spark.sql(
    "SELECT COALESCE(MAX(CardKey), 0) AS mk FROM gold.Dim_Card"
).collect()[0]['mk']

new_cards = new_cards.withColumn(
    "CardKey",
    (monotonically_increasing_id() + max_card_key + 1).cast("bigint")
)

new_cards.select(
    "CardKey", "CardID", "CustomerID", "CardType", "CardStatus",
    "ExpiryDate", "CreditLimit", "CardTier", "IsExpired",
    "AvailableCredit", "StartDate", "EndDate", "IsCurrent"
).write.format("delta").mode("append").saveAsTable("gold.Dim_Card")
print(f"Dim_Card: {new_cards.count()} new records loaded")


# ── Dim_Merchant SCD Type 2 ───────────────────────────
df_silver_merch = spark.sql("""
    SELECT DISTINCT MerchantID, MerchantName, MerchantCategory,
                    City, Country, RiskLevel, IsHighRisk, MerchantSegment
    FROM silver.merchants
    WHERE MerchantID IS NOT NULL
""")

dim_merch = DeltaTable.forName(spark, "gold.Dim_Merchant")

dim_merch.alias("t").merge(
    df_silver_merch.alias("s"),
    "t.MerchantID = s.MerchantID AND t.IsCurrent = true"
).whenMatchedUpdate(
    condition="""
        t.MerchantCategory != s.MerchantCategory OR
        t.RiskLevel        != s.RiskLevel         OR
        t.City             != s.City
    """,
    set={
        "EndDate":   "current_date()",
        "IsCurrent": "false"
    }
).execute()

existing_active_merch = spark.sql(
    "SELECT MerchantID FROM gold.Dim_Merchant WHERE IsCurrent = true"
)
new_merch = df_silver_merch.join(
    existing_active_merch, on='MerchantID', how='left_anti'
).withColumn("StartDate", current_date()) \
 .withColumn("EndDate",   lit(None).cast("date")) \
 .withColumn("IsCurrent", lit(True))

max_merch_key = spark.sql(
    "SELECT COALESCE(MAX(MerchantKey), 0) AS mk FROM gold.Dim_Merchant"
).collect()[0]['mk']

new_merch = new_merch.withColumn(
    "MerchantKey",
    (monotonically_increasing_id() + max_merch_key + 1).cast("bigint")
)

new_merch.select(
    "MerchantKey", "MerchantID", "MerchantName", "MerchantCategory",
    "City", "Country", "RiskLevel", "IsHighRisk", "MerchantSegment",
    "StartDate", "EndDate", "IsCurrent"
).write.format("delta").mode("append").saveAsTable("gold.Dim_Merchant")
print(f"Dim_Merchant: {new_merch.count()} new records loaded")


# ── Fact_Transactions ──────────────────────────────────
df_fact = spark.sql("""
    SELECT
        t.TransactionID,
        t.CustomerID,
        t.CardID,
        t.MerchantID,
        CAST(DATE_FORMAT(t.TransactionDate, 'yyyyMMdd') AS INT) AS DateKey,
        t.Amount,
        t.Currency,
        t.TransactionType,
        t.Channel,
        t.FraudScore,
        t.IsFraud,
        t.SpendCategory,
        t.HighValueIndicator,
        t.TransactionHour,
        t.DayName,
        t.SourceFileName,
        c.CustomerKey,
        ca.CardKey,
        m.MerchantKey
    FROM silver.transactions t
    LEFT JOIN gold.Dim_Customer c
        ON t.CustomerID = c.CustomerID AND c.IsCurrent = true
    LEFT JOIN gold.Dim_Card ca
        ON t.CardID = ca.CardID AND ca.IsCurrent = true
    LEFT JOIN gold.Dim_Merchant m
        ON t.MerchantID = m.MerchantID AND m.IsCurrent = true
    WHERE t.TransactionID IS NOT NULL
      AND t.Amount > 0
""")

existing_tx = spark.sql("SELECT TransactionID FROM gold.Fact_Transactions")
new_tx = df_fact.join(existing_tx, on='TransactionID', how='left_anti')

max_tx_key = spark.sql(
    "SELECT COALESCE(MAX(TransactionKey), 0) AS mk FROM gold.Fact_Transactions"
).collect()[0]['mk']

new_tx = new_tx.withColumn(
    "TransactionKey",
    (monotonically_increasing_id() + max_tx_key + 1).cast("bigint")
)

new_tx.select(
    "TransactionKey", "CustomerKey", "CardKey", "MerchantKey", "DateKey",
    "TransactionID", "Amount", "Currency", "TransactionType", "Channel",
    "FraudScore", "IsFraud", "SpendCategory", "HighValueIndicator",
    "TransactionHour", "DayName", "SourceFileName"
).write.format("delta").mode("append").saveAsTable("gold.Fact_Transactions")

print(f"Fact_Transactions: {new_tx.count()} new records loaded")
print("\nAll Gold tables loaded successfully")