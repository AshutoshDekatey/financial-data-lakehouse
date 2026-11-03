"""Delta-backed micro-batch ingestion and deterministic financial projections."""
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from .generate import FIELDS

SCHEMA = StructType([StructField(k, StringType()) for k in FIELDS + ['_corrupt_record']])

def local_spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master('local[2]').appName('financial-lakehouse')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.sql.session.timeZone', 'UTC'))
    import os
    jars = os.environ.get('LAKEHOUSE_JARS')
    if jars:
        return builder.config('spark.jars', jars).getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()

class Pipeline:
    def __init__(self, spark, root):
        self.spark, self.root = spark, root.rstrip('/')
        spark.conf.set('spark.sql.session.timeZone', 'UTC')

    def path(self, name):
        return f'{self.root}/{name}'

    def table(self, name):
        return self.spark.read.format('delta').load(self.path(name))

    def ingest(self, source, batch_id):
        if not batch_id:
            raise ValueError('batch_id must be nonempty')
        # Read whole lines first: malformed JSON remains recoverable in Bronze.
        incoming = (self.spark.read.text(source).withColumnRenamed('value', 'raw_json')
            .withColumn('batch_id', F.lit(batch_id))
            .withColumn('record_key', F.sha2(F.concat_ws('\n', 'batch_id', 'raw_json'), 256))
            .dropDuplicates(['record_key']))
        path = self.path('bronze')
        if DeltaTable.isDeltaTable(self.spark, path):
            (DeltaTable.forPath(self.spark, path).alias('t').merge(incoming.alias('s'),
                't.record_key = s.record_key').whenNotMatchedInsertAll().execute())
        else:
            incoming.write.format('delta').save(path)
        self.refresh()

    def refresh(self):
        parsed = self.table('bronze').withColumn('p', F.from_json('raw_json', SCHEMA))
        events = parsed.select('*', 'p.*').drop('p')
        events = (events.withColumn('v', F.expr('try_cast(version as bigint)'))
            .withColumn('amount', F.expr('try_cast(amount_minor as bigint)'))
            .withColumn('ts', F.expr('try_cast(event_time as timestamp)'))
            .withColumn('day', F.expr('try_cast(settlement_date as date)')))
        rules = [
            (F.col('_corrupt_record').isNotNull() | F.col('raw_json').isin('null', ''), 'malformed_json'),
            (F.col('event_id').isNull() | (F.trim('event_id') == ''), 'missing_event_id'),
            (F.col('transaction_id').isNull() | (F.trim('transaction_id') == ''), 'missing_transaction_id'),
            (F.col('account_id').isNull() | (F.trim('account_id') == ''), 'missing_account_id'),
            (~F.col('version').rlike('^[1-9][0-9]*$') | F.col('v').isNull(), 'invalid_version'),
            (~F.col('amount_minor').rlike('^[0-9]+$') | F.col('amount').isNull(), 'invalid_amount'),
            (F.col('ts').isNull(), 'invalid_event_time'),
            (F.col('day').isNull() | ~F.col('settlement_date').rlike('^[0-9]{4}-[0-9]{2}-[0-9]{2}$'), 'invalid_settlement_date'),
            (~F.col('currency').isin('USD', 'EUR', 'GBP'), 'unsupported_currency'),
            (~F.col('status').isin('SETTLED', 'VOID'), 'invalid_status'),
        ]
        events = events.withColumn('reasons', F.filter(F.array(*[
            F.when(F.coalesce(cond, F.lit(True)), F.lit(reason)) for cond, reason in rules
        ]), lambda x: x.isNotNull()))
        # Semantic identity ignores JSON formatting and batch replay.
        events = events.withColumn('payload_hash', F.sha2(F.to_json(F.struct(*FIELDS)), 256))
        conflicts = (events.groupBy('event_id').agg(F.countDistinct('payload_hash').alias('variants')))
        events = events.join(conflicts, 'event_id', 'left').withColumn('reasons',
            F.when(F.col('variants') > 1, F.array_union('reasons', F.array(F.lit('event_id_conflict'))))
             .otherwise(F.col('reasons')))
        self._write(events.filter(F.size('reasons') > 0), 'quarantine')
        valid = events.filter(F.size('reasons') == 0).dropDuplicates(['event_id'])
        self._write(valid, 'silver_events')
        window = Window.partitionBy('transaction_id').orderBy(F.desc('v'), F.desc('ts'), F.desc('event_id'))
        current = valid.withColumn('rank', F.row_number().over(window)).filter('rank = 1').drop('rank')
        self._write(current, 'silver_current')
        current.createOrReplaceTempView('settlement_current')
        gold = self.spark.sql('''SELECT day AS settlement_date, account_id, currency,
            count(*) AS transaction_count, sum(cast(amount AS DECIMAL(38,0))) AS amount_minor
            FROM settlement_current WHERE status = 'SETTLED'
            GROUP BY day, account_id, currency''')
        self._write(gold, 'gold_settlement')

    def _write(self, df, name):
        df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(self.path(name))

    def reconcile(self):
        current = self.table('silver_current').filter("status = 'SETTLED'")
        expected = current.agg(F.count('*').alias('n'), F.sum(F.col('amount').cast('decimal(38,0)')).alias('a')).first()
        actual = self.table('gold_settlement').agg(F.sum('transaction_count').alias('n'), F.sum('amount_minor').alias('a')).first()
        return {'matches': (expected.n, expected.a or 0) == (actual.n or 0, actual.a or 0),
                'settled_transactions': expected.n, 'amount_minor': str(expected.a or 0)}
