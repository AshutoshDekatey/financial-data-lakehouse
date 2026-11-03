# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Data Lakehouse v0.1
# MAGIC Independent synthetic portfolio demonstration. Use an existing classic
# MAGIC Databricks Runtime 17.3 LTS cluster; no pip Spark/Delta installation needed.

# COMMAND ----------
import os
import sys
from pathlib import Path

# Run from the notebooks directory in a Databricks Git folder.
repo = Path(os.getcwd()).parent
assert (repo / 'src/lakehouse').exists(), 'Set repo to the absolute Git folder path'
sys.path.insert(0, str(repo / 'src'))
from lakehouse.generate import generate
from lakehouse.pipeline import Pipeline

# COMMAND ----------
dbutils.widgets.text('output_root', '')
root = dbutils.widgets.get('output_root').rstrip('/')
assert root.startswith('/Volumes/'), 'Supply a writable /Volumes/catalog/schema/volume/demo path'
generate(f'{root}/input')
pipeline = Pipeline(spark, f'{root}/lake')

# COMMAND ----------
results = []
for batch in [1, 2, 2]:
    pipeline.ingest(f'{root}/input/batch_{batch:02}.jsonl', f'b{batch}')
    results.append(pipeline.reconcile())
assert [r['amount_minor'] for r in results] == ['12500', '15000', '15000']
assert all(r['matches'] for r in results)
assert pipeline.table('quarantine').count() == 2
display(pipeline.table('gold_settlement').orderBy('settlement_date'))
display(pipeline.table('quarantine').select('raw_json', 'reasons'))
print(results)
