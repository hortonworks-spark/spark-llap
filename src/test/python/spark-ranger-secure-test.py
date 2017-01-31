#!/bin/env python

import os
import sys
import unittest
import subprocess
import filecmp
import tempfile

testdb = "spark_ranger_test"
hiveJdbcUrl = 'jdbc:hive2://ctr-e126-1485243696039-11019-01-000004.hwx.site:10500/default;principal=hive/_HOST@EXAMPLE.COM'
generateGoldenFiles = False
answerPath = "../resources/answer"
dirPath = tempfile.mkdtemp()
dbs = [
    'db_full',
    'db_no',
    'db_partial',
    'db_select',
    'db_update',
    'db_create',
    'db_drop',
    'db_alter',
    'db_index',
    'db_lock',
]
tables = [
    't_full',
    't_no',
    't_partial',
    't_select',
    't_update',
    't_create',
    't_drop',
    't_alter',
    't_index',
    't_lock',
]


class SparkRangerTestSuite(unittest.TestCase):
    def out_answer_file(self, test_id):
        return "{}/{}.out_answer".format(answerPath, test_id)

    def err_answer_file(self, test_id):
        return "{}/{}.err_answer".format(answerPath, test_id)

    def out_file(self, test_id):
        return "{}/{}.out".format(dirPath, test_id)

    def err_file(self, test_id):
        return "{}/{}.err".format(dirPath, test_id)

    def execute(self, query, test_id, user='spark', check=True, verbose=False):
        if verbose or generateGoldenFiles:
            print "[{}] {} {}".format(test_id, user, query)
        try:
            p = subprocess.Popen(['beeline', '-u', self.sparkJdbcUrl + user,
                                  '--silent=true', '-e', query],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out, err = p.communicate()
        except:
            pass
        finally:
            if verbose or generateGoldenFiles:
                if len(out) > 0:
                    print out
                if len(err.split('\n')[0]) > 0:
                    print err.split('\n')[0] + "\n"
            if test_id is not None:
                f = open(self.out_file(test_id), 'w')
                f.write(out)
                f.close()
                f = open(self.err_file(test_id), 'w')
                f.write(err.split('\n')[0])
                f.close()
        if check:
            self.check_result(test_id)

    def check_result(self, test_id):
        out_new = self.out_file(test_id)
        err_new = self.err_file(test_id)
        out_answer = self.out_answer_file(test_id)
        err_answer = self.err_answer_file(test_id)
        if generateGoldenFiles:
            # Overwrite the existing answers if exist
            os.rename(out_new, out_answer)
            os.rename(err_new, err_answer)
            if os.stat(out_answer).st_size == 0:
                os.remove(out_answer)
            if os.stat(err_answer).st_size == 0:
                os.remove(err_answer)
        else:
            if os.stat(out_new).st_size == 0:
                self.assertFalse(os.path.exists(out_answer))
            else:
                self.assertTrue(filecmp.cmp(out_new, out_answer))
            os.remove(self.out_file(test_id))

            if os.stat(err_new).st_size == 0:
                self.assertFalse(os.path.exists(err_answer))
            else:
                self.assertTrue(filecmp.cmp(err_new, err_answer))
            os.remove(self.err_file(test_id))


class DbTestSuite(SparkRangerTestSuite):
    sparkJdbcUrl = 'jdbc:hive2://ctr-e126-1485243696039-11019-01-000004.hwx.site:10016/;principal=hive/_HOST@EXAMPLE.COM;hive.server2.proxy.user='

    def setUp(self):
        sqls = map(lambda db: 'DROP DATABASE IF EXISTS ' + db + ' CASCADE', dbs + [testdb])
        statements = ';'.join(sqls)
        cmd = 'beeline --silent=true -u "{}" -e \'{}\''
        os.system(cmd.format(hiveJdbcUrl, statements))

    def test_00_show(self):
        self.execute('SHOW DATABASES', 'show_db_1')
        self.execute("SHOW DATABASES LIKE 'default'", 'show_db_2')

    def test_10_create_db(self):
        for db in dbs:
            self.execute('CREATE DATABASE ' + db, 'create_db_1_' + db)
            self.execute("SHOW DATABASES LIKE '" + db + "'", 'create_db_2_' + db, 'hive')

    def test_20_drop_db(self):
        for db in dbs:
            self.execute('CREATE DATABASE ' + db, 'drop_db_1_' + db, 'hive')
            self.execute('DROP DATABASE ' + db, 'drop_db_2_' + db)
            self.execute("SHOW DATABASES LIKE '" + db + "'", 'drop_db_3_' + db, 'hive')

    def test_21_drop_db_cascade(self):
        for db in dbs:
            self.execute('CREATE DATABASE ' + db, 'drop_db_cascade_1_' + db, 'hive')
            self.execute('CREATE TABLE ' + db + '.t (a INT)', 'drop_db_cascade_2_' + db, 'hive')
            self.execute('DROP DATABASE ' + db + ' CASCADE', 'drop_db_cascade_3_' + db)
            self.execute("SHOW DATABASES LIKE '" + db + "'", 'drop_db_cascade_4_' + db, 'hive')
            self.execute('DROP DATABASE ' + db + ' CASCADE', 'drop_db_cascade_5_' + db, 'hive')


class TableTestSuite(SparkRangerTestSuite):
    sparkJdbcUrl = 'jdbc:hive2://ctr-e126-1485243696039-11019-01-000004.hwx.site:10016/' + testdb + ';principal=hive/_HOST@EXAMPLE.COM;hive.server2.proxy.user='

    def setUp(self):
        sqls = [
            'DROP DATABASE IF EXISTS {} CASCADE',
            'CREATE DATABASE {}',
            'CREATE TABLE {}.t_full (a int, b int)',
            'CREATE TABLE {}.t_no (a int, b int)',
            'CREATE TABLE {}.t_partial (a int, b int)',
            'CREATE TABLE {}.t_select (a int, b int)',
            'CREATE TABLE {}.t_update (a int, b int)',
            'CREATE TABLE {}.t_create (a int, b int)',
            'CREATE TABLE {}.t_drop (a int, b int)',
            'CREATE TABLE {}.t_alter (a int, b int)',
            'CREATE TABLE {}.t_index (a int, b int)',
            'CREATE TABLE {}.t_lock (a int, b int)',
            'CREATE TABLE {}.t_mask_and_filter (name STRING, gender STRING)',
            'INSERT INTO {}.t_full VALUES(1, 2)',
            'INSERT INTO {}.t_no VALUES(1, 2)',
            'INSERT INTO {}.t_partial VALUES(1, 2)',
            'INSERT INTO {}.t_select VALUES(1, 2)',
            'INSERT INTO {}.t_update VALUES(1, 2)',
            'INSERT INTO {}.t_create VALUES(1, 2)',
            'INSERT INTO {}.t_drop VALUES(1, 2)',
            'INSERT INTO {}.t_alter VALUES(1, 2)',
            'INSERT INTO {}.t_index VALUES(1, 2)',
            'INSERT INTO {}.t_lock VALUES(1, 2)',
            'INSERT INTO {}.t_mask_and_filter VALUES("Barack Obama", "M")',
            'INSERT INTO {}.t_mask_and_filter VALUES("Michelle Obama", "F")',
            'INSERT INTO {}.t_mask_and_filter VALUES("Hilary Clinton", "F")',
            'INSERT INTO {}.t_mask_and_filter VALUES("Donald Trump", "M")',
        ]
        statements = ';'.join(map(lambda x: x.format(testdb), sqls))
        cmd = 'beeline --silent=true -u "{}" -e \'{}\''
        os.system(cmd.format(hiveJdbcUrl, statements))

    def test_00_show(self):
        self.execute('SHOW TABLES', 'show_1', 'hive')
        self.execute('SHOW TABLES', 'show_2')
        self.execute('SHOW TABLES \'t_full\'', 'show_3')

    def test_10_desc(self):
        for t in tables:
            self.execute('DESC ' + t, 'desc_' + t)

    def test_20_create(self):
        for t in tables:
            self.execute('CREATE TABLE ' + t + '_create(a INT)', 'create_1_' + t)
            self.execute('SHOW TABLES \'' + t + '_create\'', 'create_2_' + t, 'hive')

    def test_21_create_as(self):
        for t in tables + ['t_ctas']:
            self.execute('CREATE TABLE ' + t + '_create_as AS SELECT * FROM t_full', 'create_as_1_' + t)
            self.execute('SHOW TABLES \'' + t + '_create_as\'', 'create_as_2_' + t, 'hive')

    def test_30_drop(self):
        for t in tables:
            self.execute('DROP TABLE ' + t, 'drop_1_' + t)
            self.execute('SHOW TABLES \'' + t + '\'', 'drop_2_' + t, 'hive')

    def test_40_alter_rename(self):
        for t in tables:
            self.execute('ALTER TABLE ' + t + ' RENAME TO ' + t + '_', 'alter_rename_1_' + t)
            self.execute('SHOW TABLES \'' + t + '_\'', 'alter_rename_2_' + t)

    def test_41_alter_set_tblproperties(self):
        for t in tables:
            self.execute('ALTER TABLE ' + t + ' SET TBLPROPERTIES (\'Comment\'=\'a\')', 'alter_set_tbl_1_' + t)
            self.execute('ALTER TABLE ' + t + ' SET TBLPROPERTIES (\'Comment\'=\'a\')', 'alter_set_tbl_2_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + ' UNSET TBLPROPERTIES (\'Comment\')', 'alter_set_tbl_3_' + t)

    def test_42_alter_set_serdeproperties(self):
        for t in tables:
            self.execute('ALTER TABLE ' + t + ' SET SERDEPROPERTIES (\'field.delim\'=\',\')', 'alter_set_serde_1_' + t)

    def test_43_alter_add_partition(self):
        for t in tables:
            self.execute('CREATE TABLE ' + t + '_part(a INT, b INT) PARTITIONED BY (c INT)', 'alter_add_partition_1_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part ADD PARTITION (c=1)', 'alter_add_partition_2_' + t)
            self.execute('SHOW PARTITIONS ' + t + '_part', 'alter_add_partition_3_' + t, 'hive')

    def test_44_alter_rename_partition(self):
        for t in tables:
            self.execute('CREATE TABLE ' + t + '_part(a INT, b INT) PARTITIONED BY (c INT)', 'alter_rename_partition_1_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part ADD PARTITION (c=1)', 'alter_rename_partiton_2_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part PARTITION (c=1) RENAME TO PARTITION (c=2)', 'alter_rename_partiton_3_' + t)
            self.execute('SHOW PARTITIONS ' + t + '_part', 'alter_rename_partiton_4_' + t, 'hive')

    def test_45_alter_drop_partition(self):
        for t in tables:
            self.execute('CREATE TABLE ' + t + '_part(a INT, b INT) PARTITIONED BY (c INT)', 'alter_drop_partition_1_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part ADD PARTITION (c=1)', 'alter_drop_partiton_2_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part DROP PARTITION (c=1)', 'alter_drop_partiton_3_' + t)
            self.execute('SHOW PARTITIONS ' + t + '_part', 'alter_drop_partiton_4_' + t, 'hive')

    def test_46_alter_partition_set(self):
        for t in tables:
            self.execute('CREATE TABLE ' + t + '_part(a INT, b INT) PARTITIONED BY (c INT)', 'alter_partition_set_1_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part ADD PARTITION (c=1)', 'alter_partiton_set_2_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part PARTITION (c=1) SET LOCATION \'/tmp/x\'', 'alter_partition_set_3_' + t)

    def test_47_alter_recover(self):
        for t in tables:
            self.execute('CREATE TABLE ' + t + '_part(a INT, b INT) PARTITIONED BY (c INT)', 'alter_recover_1_' + t, 'hive')
            self.execute('ALTER TABLE ' + t + '_part RECOVER PARTITIONS', 'alter_recover_2_' + t)

    # Apache Spark 2.2.0-SNAPSHOT supports this.
    # def test_4x_alter_change(self):
    #    for t in tables:
    #        self.execute('ALTER TABLE ' + t + ' CHANGE a a int COMMENT "c"', 'alter_change_1_' + t)
    #        self.execute('DESC ' + t, 'alter_change_2_' + t)

    def test_50_select(self):
        for t in tables:
            self.execute('SELECT * FROM ' + t, 'select_1_' + t)
            self.execute('SELECT a FROM ' + t, 'select_2_' + t)
            self.execute('SELECT b FROM ' + t, 'select_3_' + t)

    def test_60_select_count(self):
        for t in tables:
            self.execute('SELECT count(*) FROM ' + t, 'select_count_1_' + t)
            self.execute('SELECT count(a) FROM ' + t, 'select_count_2_' + t)
            self.execute('SELECT count(b) FROM ' + t, 'select_count_3_' + t)

    def test_70_insert(self):
        for t in tables:
            self.execute('INSERT INTO ' + t + ' VALUES(3, 4)', 'insert_1_' + t)
            self.execute('SELECT * FROM ' + t, 'insert_2_' + t, 'hive')

    def test_80_truncate(self):
        for t in tables:
            self.execute('TRUNCATE TABLE ' + t, 'truncate_1_' + t)
            self.execute('SELECT * FROM ' + t, 'truncate_2_' + t, 'hive')

    def test_90_mask(self):
        self.execute('SELECT * FROM t_mask_and_filter', 'mask_1')
        self.execute('SELECT * FROM t_mask_and_filter', 'mask_2', 'hive')

    def test_A0_filter(self):
        self.execute('SELECT * FROM t_mask_and_filter', 'filter_1')
        self.execute('SELECT * FROM t_mask_and_filter', 'filter_2', 'hive')




if __name__ == '__main__':
    if os.getenv('REGENERATE', 'false') == 'true':
        if not os.path.exists(answerPath):
            os.makedirs(answerPath)
        generateGoldenFiles = True
    unittest.main()
