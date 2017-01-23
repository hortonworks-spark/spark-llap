## PREREQUISITES

This test assumes a HDP 2.6 cluster with spark-llap library (equal to or greater than v1.0.1-2.1).

**Apache Ranger Rules**

Policy Type | Name | Database | Table | Column | Etc
-------------|-------|----------|-------|--------------|----
Access | spark_system | default | tmp_* | * | Permissions: All
Access | spark_full | spark_ranger_test | t_full* | * | Permissions: All
Access | spark_partial | spark_ranger_test | t_partial* | a | Permissions: All
Access | spark_mask_and_filter | spark_ranger_test | t_mask_and_filter | * | Permissions: select
Masking | spark_mask | spark_ranger_test | t_mask_and_filter | name | Access Types: select, Select Masking Option: partial mask:'show first 4'
Filter | spark_filter | spark_ranger_test | t_mask_and_filter | | Access Types: select, Row Level Filter: gender='M'
Access | spark_select | spark_ranger_test | t_select* | * | Permissions: select
Access | spark_update | spark_ranger_test | t_update* | * | Permissions: update
Access | spark_create | spark_ranger_test | t_create* | * | Permissions: Create
Access | spark_drop | spark_ranger_test | t_drop* | * | Permissions: Drop
Access | spark_alter | spark_ranger_test | t_alter* | * | Permissions: Alter
Access | spark_index | spark_ranger_test | t_index* | * | Permissions: Index
Access | spark_lock | spark_ranger_test | t_lock* | * | Permissions: Lock

## How to run all tests

    ./spark-ranger-test.py
    ...................
    ----------------------------------------------------------------------
    Ran 19 tests in 1928.712s
    
    OK

## How to run a single test

    ./spark-ranger-test.py SparkRangerTestSuite.test_00_show

## How to regenerate answer files for all tests

    REGENERATE=true ./spark-ranger-test.py

## How to regenerate answer files for a single test

    REGENERATE=true ./spark-ranger-test.py SparkRangerTestSuite.test_00_show

