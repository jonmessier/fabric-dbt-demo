---
description: Add a dbt unit test to a model
argument-hint: <model_name> <what the test should prove>
---
Add a unit test for: $ARGUMENTS

Read the model SQL first. Write the test in the model's YAML file under `unit_tests:` with the
smallest set of input rows that proves the behavior, including one edge case (null, negative
quantity, boundary date, duplicate key). Use `format: dict` rows and cast-compatible values for
T-SQL. For incremental models, override `is_incremental` as needed. Run
`dbt test -s <model>,test_type:unit` and show the result; if it fails, decide whether the test or
the model is wrong and say which before changing anything.
