## PREREQUISITES

This test assumes a HDP 2.6 cluster with spark-llap library (equal to or greater than v1.0.7-2.1).
Import the following files in `../resources/policies/` via Apache Ranger UI.

  - 1_spark_system.json
  - 2_db_test.json
  - 3_table_test.json
  - 4_filter_mask_test.json

**Apache Ranger Rules**

### `Access` policy for `default` database

This is for a temporary space for Spark while `INSERT INTO`.

Name         | Table | Column | Permissions
-------------|-------|--------|------------
spark_system | tmp_* | *      | All

### `Access` policy for `db_*` database

Name             | Database   | Table   | Column | Permissions
-----------------|------------|---------|--------|------------
spark_db_full    | db_full    | *       | *      | All
spark_db_partial | db_partial | t_full* | *      | All
spark_db_select  | db_select  | *       | *      | Select
spark_db_update  | db_update  | *       | *      | Update
spark_db_create  | db_create  | *       | *      | Create
spark_db_drop    | db_drop    | *       | *      | Drop
spark_db_alter   | db_alter   | *       | *      | Alter
spark_db_index   | db_index   | *       | *      | Index
spark_db_lock    | db_lock    | *       | *      | Lock

### `Access` policy for `spark_ranger_test` database

Name                  | Table             | Column | Permissions
----------------------|-------------------|--------|------------
spark_full            | t_full*           | *      | All
spark_partial         | t_partial*        | a      | All
spark_select          | t_select*         | *      | Select
spark_update          | t_update*         | *      | Update
spark_create          | t_create*         | *      | Create
spark_ctas            | t_ctas*           | *      | Create, Update
spark_drop            | t_drop*           | *      | Drop
spark_alter           | t_alter*          | *      | Alter
spark_index           | t_index*          | *      | Index
spark_lock            | t_lock*           | *      | Lock
spark_mask_and_filter | t_mask_and_filter | *      | Select

### `Masking` policy for filtering and masking in `spark_ranger_test` database

Name       | Table             | Column | Access Types | Select Masking Option
-----------|-------------------|--------|--------------|----------------------------
spark_mask | t_mask_and_filter | name   | Select       | partial mask:'show first 4'

### `Filter` policy for filtering and masking in `spark_ranger_test` database

Name         | Table             | Access Types | Row Level Filter
-------------|-------------------|--------------|-----------------
spark_filter | t_mask_and_filter | Select       | gender='M'

## How to run all tests

```
$ ./spark-ranger-test.py
......................
----------------------------------------------------------------------
Ran 22 tests in 2894.977s

OK
```

## How to run a single test suite

    ./spark-ranger-test.py DbTestSuite

## How to run a single test

    ./spark-ranger-test.py DbTestSuite.test_00_show

## How to regenerate answer files for all tests

    REGENERATE=true ./spark-ranger-test.py

## How to regenerate answer files for a single test

    REGENERATE=true ./spark-ranger-test.py DbTestSuite.test_00_show

