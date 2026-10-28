# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Data Lakehouse | Milestone 1
# MAGIC A self-contained, reproducible PySpark + Delta Lake pipeline.
# MAGIC **Bronze:** preserve one copy of each event ID. **Silver:** validate and maintain latest payment state. **Gold:** SQL-ready daily metrics.
# MAGIC Source events are embedded so no cloud storage or uploads are needed for the first execution.

# COMMAND ----------
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "financial_lakehouse"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
PREFIX = f"`{CATALOG}`.`{SCHEMA}`"
BRONZE = f"{PREFIX}.`bronze_transactions`"
SILVER = f"{PREFIX}.`silver_payments`"
QUARANTINE = f"{PREFIX}.`quarantine_events`"
GOLD = f"{PREFIX}.`gold_daily_payments`"
print("Tables will live under:", f"{CATALOG}.{SCHEMA}")

# COMMAND ----------
# Business rule: each event is a payment-state snapshot. Higher event_version
# always wins; if versions tie, later event_ts wins. SETTLED contributes to revenue.
# The same event_id replayed is NOT a new raw event.
FIELDS = ["event_id", "payment_id", "merchant_id", "raw_amount",
          "status", "event_version", "event_ts", "source_batch"]
EVENT_SCHEMA = T.StructType([T.StructField(col, T.StringType(), True) for col in FIELDS])

BATCHES = {
    "batch_01": [
        ("EVT-001", "P001", "M01", "100.00", "SETTLED", "1", "2026-10-20 09:00:00", "batch_01"),
        ("EVT-002", "P002", "M01", "50.00", "SETTLED", "1", "2026-10-20 09:05:00", "batch_01"),
        ("EVT-003", "P003", "M02", "70.00", "FAILED", "1", "2026-10-20 09:10:00", "batch_01"),
        ("EVT-004", "P004", "M02", "200.00", "SETTLED", "2", "2026-10-20 10:30:00", "batch_01"),
        ("EVT-005", "P005", "M03", "", "SETTLED", "1", "2026-10-20 09:15:00", "batch_01"),
        ("EVT-006", "P002", "M01", "50.00", "SETTLED", "1", "2026-10-20 09:05:00", "batch_01"),
    ],
    "batch_02": [
        ("EVT-007", "P001", "M01", "120.00", "SETTLED", "2", "2026-10-20 12:00:00", "batch_02"),
        ("EVT-008", "P004", "M02", "175.00", "SETTLED", "1", "2026-10-20 08:30:00", "batch_02"),
        ("EVT-009", "P006", "M03", "80.00", "SETTLED", "1", "2026-10-19 20:15:00", "batch_02"),
        ("EVT-010", "P007", "", "90.00", "SETTLED", "1", "2026-10-20 09:00:00", "batch_02"),
        ("EVT-011", "P008", "M04", "-10.00", "SETTLED", "1", "2026-10-20 09:20:00", "batch_02"),
        ("EVT-012", "P009", "M04", "oops", "FAILED", "1", "2026-10-20 09:25:00", "batch_02"),
        ("EVT-013", "P010", "M01", "20.00", "UNKNOWN", "1", "2026-10-20 09:30:00", "batch_02"),
        ("EVT-014", "P011", "M03", "15.00", "SETTLED", "1", "invalid-date", "batch_02"),
    ],
}

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
    event_id STRING, payment_id STRING, merchant_id STRING, raw_amount STRING,
    status STRING, event_version STRING, event_ts STRING,
    source_batch STRING, ingested_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    payment_id STRING, merchant_id STRING, amount_inr DECIMAL(18,2),
    status STRING, event_version INT, event_ts TIMESTAMP,
    source_event_id STRING, updated_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE} (
    event_id STRING, payment_id STRING, source_batch STRING,
    rejection_reason STRING, quarantined_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------
def ingest_and_transform_batch(batch_name):
    """Bronze event MERGE, validate that batch, quarantine, then Silver state MERGE."""
    incoming = spark.createDataFrame(BATCHES[batch_name], EVENT_SCHEMA)
    # Fail fast if the upstream source violates the required transport-level event key.
    assert incoming.filter(F.col("event_id").isNull() | (F.trim("event_id") == "")).count() == 0
    assert incoming.select("event_id").distinct().count() == incoming.count()
    incoming.withColumn("ingested_at", F.current_timestamp()).createOrReplaceTempView("incoming_events")

    spark.sql(f"""
        MERGE INTO {BRONZE} AS target
        USING incoming_events AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, payment_id, merchant_id, raw_amount, status,
            event_version, event_ts, source_batch, ingested_at
        ) VALUES (
            source.event_id, source.payment_id, source.merchant_id, source.raw_amount,
            source.status, source.event_version, source.event_ts,
            source.source_batch, source.ingested_at
        )
    """)

    # Read from committed Bronze (rather than bypassing it from raw Python rows).
    staged = (spark.table(BRONZE).where(F.col("source_batch") == batch_name)
        .withColumn("amount", F.expr("try_cast(raw_amount AS DECIMAL(18,2))"))
        .withColumn("version", F.expr("try_cast(event_version AS INT)"))
        .withColumn("timestamp", F.expr("try_cast(event_ts AS TIMESTAMP)")))

    required = lambda name: F.trim(F.coalesce(F.col(name), F.lit(""))) == ""
    reasons = [
        F.when(required("payment_id"), F.lit("missing_payment_id")),
        F.when(required("merchant_id"), F.lit("missing_merchant_id")),
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("invalid_amount")),
        F.when(~F.coalesce(F.col("status").isin("SETTLED", "FAILED"), F.lit(False)), F.lit("invalid_status")),
        F.when(F.col("version").isNull() | (F.col("version") <= 0), F.lit("invalid_version")),
        F.when(F.col("timestamp").isNull(), F.lit("invalid_timestamp")),
    ]
    staged = staged.withColumn("rejection_reason", F.concat_ws("|", *reasons))
    rejected = (staged.filter(F.col("rejection_reason") != "")
        .select("event_id", "payment_id", "source_batch", "rejection_reason")
        .withColumn("quarantined_at", F.current_timestamp()))
    rejected.createOrReplaceTempView("rejected_events")
    spark.sql(f"""
        MERGE INTO {QUARANTINE} t USING rejected_events s ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, payment_id, source_batch, rejection_reason, quarantined_at
        ) VALUES (s.event_id, s.payment_id, s.source_batch, s.rejection_reason, s.quarantined_at)
    """)

    order = Window.partitionBy("payment_id").orderBy(
        F.col("version").desc(), F.col("timestamp").desc(), F.col("event_id").desc())
    candidates = (staged.filter(F.col("rejection_reason") == "")
        .withColumn("row_rank", F.row_number().over(order))
        .filter(F.col("row_rank") == 1)
        .select("payment_id", "merchant_id", F.col("amount").alias("amount_inr"),
                "status", F.col("version").alias("event_version"),
                F.col("timestamp").alias("event_ts"), F.col("event_id").alias("source_event_id"))
        .withColumn("updated_at", F.current_timestamp()))
    candidates.createOrReplaceTempView("silver_updates")
    spark.sql(f"""
        MERGE INTO {SILVER} AS t USING silver_updates AS s
        ON t.payment_id = s.payment_id
        WHEN MATCHED AND (
            s.event_version > t.event_version OR
            (s.event_version = t.event_version AND s.event_ts > t.event_ts)
        ) THEN UPDATE SET
            t.merchant_id = s.merchant_id, t.amount_inr = s.amount_inr,
            t.status = s.status, t.event_version = s.event_version,
            t.event_ts = s.event_ts, t.source_event_id = s.source_event_id,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
            payment_id, merchant_id, amount_inr, status,
            event_version, event_ts, source_event_id, updated_at
        ) VALUES (
            s.payment_id, s.merchant_id, s.amount_inr, s.status,
            s.event_version, s.event_ts, s.source_event_id, s.updated_at
        )
    """)
    print(f"Processed {batch_name}")


def build_gold():
    # Milestone 1 intentionally does a full refresh of the small Gold aggregate.
    # Bronze + Silver use MERGE. Incremental Gold and retractions come later.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {GOLD} USING DELTA AS
        SELECT CAST(event_ts AS DATE) AS business_date, merchant_id,
            SUM(CASE WHEN status = 'SETTLED' THEN amount_inr
                     ELSE CAST(0 AS DECIMAL(18,2)) END) AS settled_total_inr,
            SUM(CASE WHEN status = 'SETTLED' THEN 1 ELSE 0 END) AS settled_count,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
        FROM {SILVER}
        GROUP BY CAST(event_ts AS DATE), merchant_id
    """)


def snapshot():
    return {
        "bronze": spark.table(BRONZE).count(),
        "quarantine": spark.table(QUARANTINE).count(),
        "silver": spark.table(SILVER).count(),
        "gold": spark.table(GOLD).count(),
        "settled_total": spark.table(GOLD).agg(F.sum("settled_total_inr")).first()[0],
    }

# COMMAND ----------
# Pass 1: batch 01 arrives, then batch 02 containing corrections and late events.
for name in BATCHES:
    ingest_and_transform_batch(name)
build_gold()
first = snapshot()
print("First pass:", first)

# Pass 2: exact input replay. Totals must remain identical.
for name in BATCHES:
    ingest_and_transform_batch(name)
build_gold()
second = snapshot()
print("After replay:", second)
assert first == second, "Replay changed business results!"
assert second == {
    "bronze": 14,
    "quarantine": 6,
    "silver": 5,
    "gold": 3,
    "settled_total": Decimal("450.00"),
}, f"Unexpected aggregate result: {second}"

payments = {row["payment_id"]: row for row in spark.table(SILVER).collect()}
assert payments["P001"]["amount_inr"] == Decimal("120.00"), "Correction not applied"
assert payments["P004"]["amount_inr"] == Decimal("200.00"), "Late old version overwrote newer state"
assert payments["P003"]["status"] == "FAILED"
assert sum(1 for p in payments.values() if p["status"] == "SETTLED") == 4
print("PASS: corrected amount, late event ordering, quarantine, and replay stability")

# COMMAND ----------
print("Current Silver payments")
display(spark.table(SILVER).orderBy("payment_id"))
print("Gold daily reporting")
display(spark.table(GOLD).orderBy("business_date", "merchant_id"))
print("Quarantine")
display(spark.table(QUARANTINE).orderBy("event_id"))

# COMMAND ----------
# MAGIC %md
# MAGIC **Expected result:** Bronze 14, Quarantine 6, Silver 5, Gold 3; total settled INR 450.00.
# MAGIC Re-run **Run all** to verify that Bronze event IDs and Silver payment states do not duplicate.
# MAGIC The notebook is a demonstration, not a production-grade ingestion solution. See README for known limits.
