import json
import sys
from .generate import generate
from .pipeline import Pipeline, local_spark

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else 'artifacts/demo'
    generate(f'{root}/input')
    spark = local_spark()
    try:
        p = Pipeline(spark, f'{root}/lake')
        evidence = []
        for batch in [1, 2, 2]:
            p.ingest(f'{root}/input/batch_{batch:02}.jsonl', f'b{batch}')
            evidence.append({'batch': batch, **p.reconcile()})
        print(json.dumps(evidence, indent=2))
        p.table('gold_settlement').orderBy('settlement_date').show()
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
