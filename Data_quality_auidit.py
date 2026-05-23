print("=" * 60)
print("  DATA QUALITY REPORT")
print("=" * 60)


# ── BRONZE LAYER ───────────────────────────────────────
print("\nBRONZE LAYER — Raw row counts")
print("-" * 40)
for t in ['bronze.customers', 'bronze.cards',
          'bronze.merchants', 'bronze.transactions']:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {t}").collect()[0]['n']
    print(f"  {t}: {count} rows")


# ── SILVER LAYER ───────────────────────────────────────
print("\nSILVER LAYER — Cleansed row counts")
print("-" * 40)
for t in ['silver.customers', 'silver.cards',
          'silver.merchants', 'silver.transactions', 'silver.dates']:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {t}").collect()[0]['n']
    print(f"  {t}: {count} rows")


# ── GOLD LAYER ────────────────────────────────────────
print("\nGOLD LAYER — Dimensional row counts")
print("-" * 40)
for t in ['gold.Dim_Customer', 'gold.Dim_Card', 'gold.Dim_Merchant',
          'gold.Dim_Date', 'gold.Fact_Transactions']:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {t}").collect()[0]['n']
    print(f"  {t}: {count} rows")


# ── NULL CHECKS ───────────────────────────────────────
print("\nNULL CHECKS — Primary keys")
print("-" * 40)

checks = {
    "bronze.customers  — CustomerID":    "SELECT COUNT(*) AS n FROM bronze.customers WHERE CustomerID IS NULL",
    "bronze.cards      — CardID":        "SELECT COUNT(*) AS n FROM bronze.cards WHERE CardID IS NULL",
    "bronze.merchants  — MerchantID":    "SELECT COUNT(*) AS n FROM bronze.merchants WHERE MerchantID IS NULL",
    "bronze.transactions — TransactionID": "SELECT COUNT(*) AS n FROM bronze.transactions WHERE TransactionID IS NULL",
    "silver.customers  — CustomerID":    "SELECT COUNT(*) AS n FROM silver.customers WHERE CustomerID IS NULL",
    "silver.cards      — CardID":        "SELECT COUNT(*) AS n FROM silver.cards WHERE CardID IS NULL",
    "silver.transactions — TransactionID": "SELECT COUNT(*) AS n FROM silver.transactions WHERE TransactionID IS NULL",
}

for label, query in checks.items():
    result = spark.sql(query).collect()[0]['n']
    status = "PASS" if result == 0 else f"FAIL — {result} nulls found"
    print(f"  {label}: {status}")


# ── DUPLICATE CHECKS ──────────────────────────────────
print("\nDUPLICATE CHECKS")
print("-" * 40)

dup_checks = {
    "silver.customers  — CustomerID":      "SELECT COUNT(*) - COUNT(DISTINCT CustomerID) AS n FROM silver.customers",
    "silver.cards      — CardID":          "SELECT COUNT(*) - COUNT(DISTINCT CardID) AS n FROM silver.cards",
    "silver.merchants  — MerchantID":      "SELECT COUNT(*) - COUNT(DISTINCT MerchantID) AS n FROM silver.merchants",
    "silver.transactions — TransactionID": "SELECT COUNT(*) - COUNT(DISTINCT TransactionID) AS n FROM silver.transactions",
    "gold.Fact_Transactions — TransactionID": "SELECT COUNT(*) - COUNT(DISTINCT TransactionID) AS n FROM gold.Fact_Transactions",
}

for label, query in dup_checks.items():
    result = spark.sql(query).collect()[0]['n']
    status = "PASS" if result == 0 else f"FAIL — {result} duplicates found"
    print(f"  {label}: {status}")


# ── INVALID VALUE CHECKS ──────────────────────────────
print("\nINVALID VALUE CHECKS")
print("-" * 40)

invalid_checks = {
    "silver.customers — Age between 18-100": """
        SELECT COUNT(*) AS n FROM silver.customers
        WHERE Age < 18 OR Age > 100
    """,
    "silver.cards — CreditLimit > 0": """
        SELECT COUNT(*) AS n FROM silver.cards
        WHERE CreditLimit <= 0 OR CreditLimit IS NULL
    """,
    "silver.transactions — Amount > 0": """
        SELECT COUNT(*) AS n FROM silver.transactions
        WHERE Amount <= 0 OR Amount IS NULL
    """,
    "silver.transactions — FraudScore 0-1": """
        SELECT COUNT(*) AS n FROM silver.transactions
        WHERE FraudScore < 0 OR FraudScore > 1
    """,
    "silver.transactions — Future dates": """
        SELECT COUNT(*) AS n FROM silver.transactions
        WHERE TransactionDate > CURRENT_DATE()
    """,
    "silver.customers — Valid email": """
        SELECT COUNT(*) AS n FROM silver.customers
        WHERE IsValidEmail = false OR Email IS NULL
    """,
}

for label, query in invalid_checks.items():
    result = spark.sql(query).collect()[0]['n']
    status = "PASS" if result == 0 else f"FAIL — {result} invalid records"
    print(f"  {label}: {status}")


# ── REFERENTIAL INTEGRITY ─────────────────────────────
print("\nREFERENTIAL INTEGRITY — Fact to Dimension joins")
print("-" * 40)

ref_checks = {
    "Fact → Dim_Customer (CustomerKey)": """
        SELECT COUNT(*) AS n FROM gold.Fact_Transactions
        WHERE CustomerKey IS NULL
    """,
    "Fact → Dim_Card (CardKey)": """
        SELECT COUNT(*) AS n FROM gold.Fact_Transactions
        WHERE CardKey IS NULL
    """,
    "Fact → Dim_Merchant (MerchantKey)": """
        SELECT COUNT(*) AS n FROM gold.Fact_Transactions
        WHERE MerchantKey IS NULL
    """,
    "Fact → Dim_Date (DateKey)": """
        SELECT COUNT(*) AS n FROM gold.Fact_Transactions
        WHERE DateKey IS NULL
    """,
}

for label, query in ref_checks.items():
    result = spark.sql(query).collect()[0]['n']
    status = "PASS" if result == 0 else f"FAIL — {result} unmatched keys"
    print(f"  {label}: {status}")


# ── SCD TYPE 2 CHECKS ─────────────────────────────────
print("\nSCD TYPE 2 CHECKS")
print("-" * 40)

scd_checks = {
    "Dim_Customer — one active record per CustomerID": """
        SELECT COUNT(*) AS n FROM (
            SELECT CustomerID, COUNT(*) AS cnt
            FROM gold.Dim_Customer
            WHERE IsCurrent = true
            GROUP BY CustomerID
            HAVING COUNT(*) > 1
        )
    """,
    "Dim_Card — one active record per CardID": """
        SELECT COUNT(*) AS n FROM (
            SELECT CardID, COUNT(*) AS cnt
            FROM gold.Dim_Card
            WHERE IsCurrent = true
            GROUP BY CardID
            HAVING COUNT(*) > 1
        )
    """,
    "Dim_Merchant — one active record per MerchantID": """
        SELECT COUNT(*) AS n FROM (
            SELECT MerchantID, COUNT(*) AS cnt
            FROM gold.Dim_Merchant
            WHERE IsCurrent = true
            GROUP BY MerchantID
            HAVING COUNT(*) > 1
        )
    """,
    "Dim_Customer — expired records have EndDate set": """
        SELECT COUNT(*) AS n FROM gold.Dim_Customer
        WHERE IsCurrent = false AND EndDate IS NULL
    """,
    "Dim_Customer — active records have NULL EndDate": """
        SELECT COUNT(*) AS n FROM gold.Dim_Customer
        WHERE IsCurrent = true AND EndDate IS NOT NULL
    """,
}

for label, query in scd_checks.items():
    result = spark.sql(query).collect()[0]['n']
    status = "PASS" if result == 0 else f"FAIL — {result} violations"
    print(f"  {label}: {status}")


# ── SCD TYPE 2 HISTORY SAMPLE ─────────────────────────
print("\nSCD TYPE 2 SAMPLE — Customers with history")
print("-" * 40)
spark.sql("""
    SELECT CustomerID, FullName, City, Age,
           StartDate, EndDate, IsCurrent,
           CASE WHEN IsCurrent = true THEN 'Active'
                ELSE 'Expired' END AS RecordStatus
    FROM gold.Dim_Customer
    WHERE CustomerID IN (
        SELECT CustomerID FROM gold.Dim_Customer
        GROUP BY CustomerID HAVING COUNT(*) > 1
    )
    ORDER BY CustomerID, StartDate
    LIMIT 10
""").show(truncate=False)


# ── FRAUD SUMMARY ─────────────────────────────────────
print("\nFRAUD SUMMARY")
print("-" * 40)
spark.sql("""
    SELECT
        COUNT(*)                                              AS Total_Transactions,
        SUM(CASE WHEN IsFraud = true THEN 1 ELSE 0 END)      AS Fraud_Count,
        ROUND(
            SUM(CASE WHEN IsFraud = true THEN 1 ELSE 0 END)
            / COUNT(*) * 100, 2
        )                                                     AS Fraud_Rate_Pct,
        ROUND(SUM(CASE WHEN IsFraud = true THEN Amount
                       ELSE 0 END), 2)                       AS Fraud_Amount,
        ROUND(AVG(FraudScore), 4)                            AS Avg_FraudScore
    FROM gold.Fact_Transactions
""").show()


print("\n" + "=" * 60)
print("  DATA QUALITY REPORT COMPLETE")
print("=" * 60)