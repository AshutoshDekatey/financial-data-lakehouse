# Development instructions

This is an independent synthetic portfolio project, not a bank's production system.
Use this task's existing checkout; do not create worktrees unless requested.
Use Python 3.10+ and Java 17+ (local validation uses Python 3.12/Java 21).
Install `pip install -e '.[test]'` in a virtual environment. Run `python -m pytest -q`.
Behind the cloud proxy, follow README's verified JAR download instructions.
Keep monetary values in integer minor units; never aggregate across currencies.
Preserve raw rejected input and explicit reasons. Add regression coverage for semantic changes.
Tests in tests/ execute local Spark/Delta, not Databricks. Do not claim remote execution
without actual evidence. Never add real financial data, credentials, paid resources,
or deploy cloud infrastructure without authorization. Do not commit generated Delta tables.
