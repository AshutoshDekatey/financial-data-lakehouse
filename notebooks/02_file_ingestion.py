# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Data Lakehouse | Milestone 2: File-backed ingestion
# MAGIC Reads CSV files **from a Unity Catalog managed volume**, instead of embedded Python fixtures.
# MAGIC Creates separate M2 Delta tables so the verified M1 tables remain unchanged.
# MAGIC This notebook expects `batch_01.csv`, `batch_02.csv`, and `batch_03.csv` in the volume.
# MAGIC All events are synthetic. Use **Run all** once uploaded, then run again to test replay stability.

# COMMAND ----------
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CATALOG = spark.sql('SELECT current_catalog()').first()[0]
SCHEMA = 'financial_lakehouse'
VOLUME = 'landing_files'
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`')
PREFIX = f'`{CATALOG}`.`{SCHEMA}`'
VOLUME_PATH = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'
BRONZE = f'{PREFIX}.`bronze_file_transactions`'
SILVER = f'{PREFIX}.`silver_file_payments`'
QUARANTINE = f'{PREFIX}.`quarantine_file_events`'
GOLD = f'{PREFIX}.`gold_file_daily_payments`'
print('Upload CSV files to:', VOLUME_PATH)
print('Output table prefix:', f'{CATALOG}.{SCHEMA}')

# COMMAND ----------
# Source columns are strings in Bronze. Validated types are assigned in Silver.
FIELDS = ['event_id', 'payment_id', 'merchant_id', 'raw_amount',
          'status', 'event_version', 'event_ts', 'source_batch']
RAW_SCHEMA = T.StructType([T.StructField(name, T.StringType(), True) for name in FIELDS])

spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {BRONZE} (
        event_id STRING, payment_id STRING, merchant_id STRING, raw_amount STRING,
        status STRING, event_version STRING, event_ts STRING, source_batch STRING,
        source_file STRING, ingested_at TIMESTAMP
    ) USING DELTA
''')
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {SILVER} (
        payment_id STRING, merchant_id STRING, amount_inr DECIMAL(18,2),
        status STRING, event_version INT, event_ts TIMESTAMP,
        source_event_id STRING, updated_at TIMESTAMP
    ) USING DELTA
''')
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {QUARANTINE} (
        event_id STRING, payment_id STRING, source_batch STRING,
        rejection_reason STRING, source_file STRING, quarantined_at TIMESTAMP
    ) USING DELTA
''')

# COMMAND ----------
def ingest_file(batch_name):
    """Load one CSV from the governed volume and update Bronze, Quarantine and Silver.

    This deliberately reprocesses the committed Bronze rows for the given batch on
    retries. Inserting by unique event_id keeps Bronze idempotent; reprocessing
    protects against a failure after Bronze MERGE and before Silver MERGE.
    """
    path = f'{VOLUME_PATH}/{batch_name}.csv'
    source = (spark.read.format('csv').option('header', True)
              .option('mode', 'FAILFAST').schema(RAW_SCHEMA).load(path))

    count = source.count()
    if count == 0:
        raise ValueError(f'Empty input file: {path}')
    if source.filter(F.col('event_id').isNull() | (F.trim('event_id') == '')).limit(1).count():
        raise ValueError(f'Missing event_id in {path}')
    if source.groupBy('event_id').count().filter(F.col('count') > 1).limit(1).count():
        raise ValueError(f'Duplicate event_id within input file: {path}')
    if source.filter(F.col('source_batch').isNull() |
                     (F.col('source_batch') != batch_name)).limit(1).count():
        raise ValueError(f'Source batch does not match filename: {path}')

    # Guard: the same event_id cannot be silently reused for DIFFERENT source data.
    existing = spark.table(BRONZE).select(*FIELDS)
    conflicts = source.alias('s').join(existing.alias('t'), 'event_id', 'inner')
    same_payload = None
    for c in FIELDS[1:]:
        compare = F.col(f's.{c}').eqNullSafe(F.col(f't.{c}'))
        same_payload = compare if same_payload is None else (same_payload & compare)
    if conflicts.filter(~same_payload).limit(1).count():
        raise ValueError(f'Event ID reused with changed payload: {path}')

    (source.withColumn('source_file', F.lit(path))
           .withColumn('ingested_at', F.current_timestamp())
           .createOrReplaceTempView('m2_incoming'))
    spark.sql(f'''
        MERGE INTO {BRONZE} t USING m2_incoming s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, payment_id, merchant_id, raw_amount, status,
            event_version, event_ts, source_batch, source_file, ingested_at
        ) VALUES (
            s.event_id, s.payment_id, s.merchant_id, s.raw_amount, s.status,
            s.event_version, s.event_ts, s.source_batch, s.source_file, s.ingested_at
        )
    ''')

    staged = (spark.table(BRONZE).where(F.col('source_batch') == batch_name)
              .withColumn('amount', F.expr('try_cast(raw_amount AS DECIMAL(18,2))'))
              .withColumn('version', F.expr('try_cast(event_version AS INT)'))
              .withColumn('timestamp', F.expr('try_cast(event_ts AS TIMESTAMP)')))

    def missing(name):
        return F.trim(F.coalesce(F.col(name), F.lit(''))) == ''

    checks = [
        F.when(missing('payment_id'), F.lit('missing_payment_id')),
        F.when(missing('merchant_id'), F.lit('missing_merchant_id')),
        F.when(F.col('amount').isNull() | (F.col('amount') <= 0), F.lit('invalid_amount')),
        F.when(~F.coalesce(F.col('status').isin('SETTLED', 'FAILED'), F.lit(False)),
               F.lit('invalid_status')),
        F.when(F.col('version').isNull() | (F.col('version') <= 0), F.lit('invalid_version')),
        F.when(F.col('timestamp').isNull(), F.lit('invalid_timestamp')),
    ]
    staged = staged.withColumn('rejection_reason', F.concat_ws('|', *checks))
    (staged.filter(F.col('rejection_reason') != '')
           .select('event_id', 'payment_id', 'source_batch', 'rejection_reason', 'source_file')
           .withColumn('quarantined_at', F.current_timestamp())
           .createOrReplaceTempView('m2_rejected'))
    spark.sql(f'''
        MERGE INTO {QUARANTINE} t USING m2_rejected s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, payment_id, source_batch, rejection_reason, source_file, quarantined_at
        ) VALUES (
            s.event_id, s.payment_id, s.source_batch, s.rejection_reason,
            s.source_file, s.quarantined_at
        )
    ''')

    ranking = Window.partitionBy('payment_id').orderBy(
        F.col('version').desc(), F.col('timestamp').desc(), F.col('event_id').desc())
    (staged.filter(F.col('rejection_reason') == '')
           .withColumn('rank', F.row_number().over(ranking))
           .filter(F.col('rank') == 1)
           .select('payment_id', 'merchant_id', F.col('amount').alias('amount_inr'),
                   'status', F.col('version').alias('event_version'),
                   F.col('timestamp').alias('event_ts'), F.col('event_id').alias('source_event_id'))
           .withColumn('updated_at', F.current_timestamp())
           .createOrReplaceTempView('m2_silver_updates'))
    spark.sql(f'''
        MERGE INTO {SILVER} t USING m2_silver_updates s
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
    ''')
    print(f'Processed file: {batch_name} ({count} source rows)')


def refresh_gold():
    # A full rebuild is intentional: changed payment state can retract an old
    # merchant-date contribution. M3 will add reconciliation and recovery checks.
    spark.sql(f'''
        CREATE OR REPLACE TABLE {GOLD} USING DELTA AS
        SELECT CAST(event_ts AS DATE) AS business_date, merchant_id,
            SUM(CASE WHEN status = 'SETTLED' THEN amount_inr
                     ELSE CAST(0 AS DECIMAL(18,2)) END) AS settled_total_inr,
            SUM(CASE WHEN status = 'SETTLED' THEN 1 ELSE 0 END) AS settled_count,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
        FROM {SILVER}
        GROUP BY CAST(event_ts AS DATE), merchant_id
    ''')


def snapshot():
    return {
        'bronze': spark.table(BRONZE).count(),
        'quarantine': spark.table(QUARANTINE).count(),
        'silver': spark.table(SILVER).count(),
        'gold': spark.table(GOLD).count(),
        'settled_total': spark.table(GOLD).agg(F.sum('settled_total_inr')).first()[0],
    }

# COMMAND ----------
# Demonstration in ordered arrivals: two original files, then a genuinely new third file.
for batch_name in ['batch_01', 'batch_02']:
    ingest_file(batch_name)
refresh_gold()
initial = snapshot()
print('After batch 01 + 02:', initial)

ingest_file('batch_03')
refresh_gold()
after_increment = snapshot()
print('After new batch 03:', after_increment)

assert after_increment == {
    'bronze': 20, 'quarantine': 7, 'silver': 6,
    'gold': 6, 'settled_total': Decimal('550.00')
}, f'Unexpected incremental result: {after_increment}'

# Reprocess every source file to demonstrate idempotent retries.
for batch_name in ['batch_01', 'batch_02', 'batch_03']:
    ingest_file(batch_name)
refresh_gold()
after_replay = snapshot()
print('After all-file replay:', after_replay)
assert after_replay == after_increment, 'Replay changed the business result!'

payments = {r['payment_id']: r for r in spark.table(SILVER).collect()}
assert payments['P001']['amount_inr'] == Decimal('120.00')
assert payments['P002']['amount_inr'] == Decimal('55.00')
assert payments['P003']['status'] == 'SETTLED'
assert payments['P004']['amount_inr'] == Decimal('200.00')
assert payments['P012']['amount_inr'] == Decimal('25.00')
assert spark.table(SILVER).filter(F.col('status') == 'SETTLED').count() == 6
assert spark.table(QUARANTINE).filter(F.col('event_id') == 'EVT-019').count() == 1
print('PASS: CSV ingestion, corrections, late events, changed status, quarantine and replay')

# COMMAND ----------
print('Bronze source lineage')
display(spark.table(BRONZE).select('event_id', 'source_batch', 'source_file', 'ingested_at')
        .orderBy('event_id'))
print('Silver current payment state')
display(spark.table(SILVER).orderBy('payment_id'))
print('Gold daily merchant reporting')
display(spark.table(GOLD).orderBy('business_date', 'merchant_id'))
print('Invalid source events')
display(spark.table(QUARANTINE).orderBy('event_id'))

# COMMAND ----------
# MAGIC %md
# MAGIC ## M2 scope and limitations
# MAGIC **Verified only when run in your Databricks workspace.** No production or streaming claims.
# MAGIC - CSV files are manually uploaded to a Unity Catalog volume; notebooks use file paths.
# MAGIC - Bronze Delta MERGE is idempotent by `event_id`; corrections resolve by payment version.
# MAGIC - Silver is processed batch-by-batch; Gold is rebuilt from the latest Silver state.
# MAGIC - Cross-batch conflicts with equal version and timestamp are not yet rejected.
# MAGIC - If a batch is absent, the run fails rather than silently skipping it.
# MAGIC - Multi-table writes are not atomic as one transaction; detailed crash recovery remains M3.
