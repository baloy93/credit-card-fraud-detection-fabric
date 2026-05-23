spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.Dim_Customer (
    CustomerKey  BIGINT,
    CustomerID   STRING,
    FullName     STRING,
    Gender       STRING,
    City         STRING,
    Country      STRING,
    Email        STRING,
    DateOfBirth  DATE,
    Age          INT,
    AgeGroup     STRING,
    IsValidEmail BOOLEAN,
    StartDate    DATE,
    EndDate      DATE,
    IsCurrent    BOOLEAN
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.Dim_Card (
    CardKey         BIGINT,
    CardID          STRING,
    CustomerID      STRING,
    CardType        STRING,
    CardStatus      STRING,
    ExpiryDate      STRING,
    CreditLimit     DECIMAL(18,2),
    CardTier        STRING,
    IsExpired       BOOLEAN,
    AvailableCredit DECIMAL(18,2),
    StartDate       DATE,
    EndDate         DATE,
    IsCurrent       BOOLEAN
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.Dim_Merchant (
    MerchantKey      BIGINT,
    MerchantID       STRING,
    MerchantName     STRING,
    MerchantCategory STRING,
    City             STRING,
    Country          STRING,
    RiskLevel        STRING,
    IsHighRisk       BOOLEAN,
    MerchantSegment  STRING,
    StartDate        DATE,
    EndDate          DATE,
    IsCurrent        BOOLEAN
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.Dim_Date (
    DateKey   INT,
    FullDate  DATE,
    Day       INT,
    Month     INT,
    Year      INT,
    Quarter   INT,
    WeekDay   STRING,
    IsWeekend BOOLEAN
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.Fact_Transactions (
    TransactionKey     BIGINT,
    CustomerKey        BIGINT,
    CardKey            BIGINT,
    MerchantKey        BIGINT,
    DateKey            INT,
    TransactionID      STRING,
    Amount             DECIMAL(18,2),
    Currency           STRING,
    TransactionType    STRING,
    Channel            STRING,
    FraudScore         DECIMAL(5,2),
    IsFraud            BOOLEAN,
    SpendCategory      STRING,
    HighValueIndicator BOOLEAN,
    TransactionHour    INT,
    DayName            STRING,
    SourceFileName     STRING
) USING DELTA
""")

print("All Gold tables created")