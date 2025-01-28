/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Integration test for user defined functions.
 */
public class UserDefinedFunctionsIntegrationTest extends AbstractBasicIntegrationTest {
    /** Log listener. */
    private static final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith("0"))
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** */
    @Test
    public void testFunctions() throws Exception {
        // Cache with impicit schema.
        IgniteCache<Integer, Employer> emp1 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp1")
            .setSqlFunctionClasses(AddFunctionsLibrary.class, MulFunctionsLibrary.class)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp1")))
        );

        // Cache with explicit custom schema.
        IgniteCache<Integer, Employer> emp2 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp2")
            .setSqlFunctionClasses(AddFunctionsLibrary.class)
            .setSqlSchema("emp2_schema")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp2")))
        );

        // Cache with PUBLIC schema.
        IgniteCache<Integer, Employer> emp3 = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp3")
            .setSqlFunctionClasses(OtherFunctionsLibrary.class)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp3")))
        );

        emp1.put(1, new Employer("Igor1", 1d));
        emp1.put(2, new Employer("Roman1", 2d));

        emp2.put(1, new Employer("Igor2", 10d));
        emp2.put(2, new Employer("Roman2", 20d));

        emp3.put(1, new Employer("Igor3", 100d));
        emp3.put(2, new Employer("Roman3", 200d));

        awaitPartitionMapExchange();

        assertQuery("SELECT \"emp1\".add(1, 2)").returns(3.0d).check();
        assertQuery("SELECT \"emp1\".add(1, 2, 3)").returns(6).check();
        assertQuery("SELECT \"emp1\".add(1, 2, 3, 4)").returns(10).check();
        assertQuery("SELECT \"emp1\".mul(1, 2)").returns(2).check();
        assertQuery("SELECT \"emp1\".mul(1, 2, 3)").returns(6).check();
        assertQuery("SELECT \"emp1\".test(1, 2, 2)").returns(true).check();
        assertQuery("SELECT \"emp1\".test(1, 2, 3)").returns(false).check();
        assertQuery("SELECT EMP2_SCHEMA.add(1, 2)").returns(3d).check();
        assertQuery("SELECT EMP2_SCHEMA.add(1, 2, 3)").returns(6).check();
        assertQuery("SELECT EMP2_SCHEMA.add(1, 2, 3, 4)").returns(10).check();
        assertQuery("SELECT sq(4)").returns(16d).check();
        assertQuery("SELECT echo('test')").returns("test").check();
        assertQuery("SELECT sq(salary) FROM emp3").returns(10_000d).returns(40_000d).check();
        assertQuery("SELECT echo(name) FROM emp3").returns("Igor3").returns("Roman3").check();
        assertQuery("SELECT sq(salary) FROM EMP2_SCHEMA.emp2").returns(100d).returns(400d).check();
        assertQuery("SELECT sq(salary) FROM \"emp1\".emp1").returns(1d).returns(4d).check();
        assertQuery("SELECT echo(?)").withParams("test").returns("test").check(); // Check type inference.
        assertThrows("SELECT add(1, 2)");
        assertThrows("SELECT mul(1, 2)");
        assertThrows("SELECT EMP2_SCHEMA.mul(1, 2)");
        assertThrows("SELECT EMP2_SCHEMA.sq(1)");

        client.cache("emp1").destroy();
        awaitPartitionMapExchange();

        assertThrows("SELECT \"emp1\".add(1, 2)");
        assertQuery("SELECT EMP2_SCHEMA.add(1, 2)").returns(3d).check();

        client.cache("emp2").destroy();
        awaitPartitionMapExchange();

        assertThrows("SELECT EMP2_SCHEMA.add(1, 2)");
        assertQuery("SELECT sq(4)").returns(16d).check();

        client.cache("emp3").destroy();
        awaitPartitionMapExchange();

        // PUBLIC schema is predefined and not dropped on cache destroy.
        assertQuery("SELECT sq(4)").returns(16d).check();
    }

    /** */
    @Test
    public void testTableFunctions() throws Exception {
        IgniteCache<Integer, Employer> emp = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp")
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(TableFunctions.class)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp")))
        );

        emp.put(1, new Employer("Igor1", 1d));
        emp.put(2, new Employer("Roman1", 2d));

        awaitPartitionMapExchange();

        assertQuery("SELECT * from tbl_fun_it(?)").withParams(1)
            .returns(2, 3, 4)
            .returns(5, 6, 7)
            .returns(8, 9, 10)
            .check();

        assertQuery("SELECT * from tbl_fun_it(?) WHERE COL_1=4").withParams(2)
            .returns(3, 4, 5)
            .check();

        assertQuery("SELECT COL_0, COL_2 from tbl_fun_it(?) WHERE COL_1=3").withParams(1)
            .returns(2, 4)
            .check();

        assertQuery("SELECT * from tbl_fun_arr(?)").withParams(1)
            .returns(2, 3, 4)
            .returns(5, 6, 7)
            .returns(8, 9, 10)
            .check();

        assertQuery("SELECT * from tbl_fun_arr_and_it(1) WHERE COL_0>4 AND COL_1>? AND COL_2>6").withParams(5)
            .returns(5, 6, 7)
            .returns(8, 9, 10)
            .check();

        assertQuery("SELECT * from tbl_fun_boxing(1, ?, ?, 4.0::FLOAT)").withParams(1, 4.0d)
            .returns(1, 1, 4.0d, 4.0d)
            .check();

        assertQuery("SELECT * from tbl_fun_boxing(1, 1, 2, 2)")
            .returns(1, 1, 2.0d, 2.0d)
            .check();

        assertQuery("SELECT * from tbl_fun_boxing(?, ?, ?, ?)").withParams(1, 1, 2.0d, 2.0d)
            .returns(1, 1, 2.0d, 2.0d)
            .check();

        assertQuery("SELECT STR_COL from tbl_fun_col_names(1001) where INT_COL>1000")
            .returns("1001")
            .returns("empty")
            .check();

        assertQuery("SELECT * from emp WHERE SALARY >= (SELECT COL_0 from tbl_fun_it(1) WHERE COL_1=3)")
            .returns("Roman1", 2d)
            .check();
    }

    /** */
    @Test
    public void testIncorrentTableFunctions() throws Exception {
        LogListener logChecker0 = LogListener.matches("One or more column names is not unique")
            .andMatches("either be empty or match the number of column types")
            .andMatches("The method is expected to return a collection (iteratable)")
            .andMatches("Column types cannot be empty")
            .build();

        listeningLog.registerListener(logChecker0);

        IgniteCache<Integer, Employer> emp = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp")
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(IncorrectTableFunctions.class)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp")))
        );

        emp.put(1, new Employer("Igor1", 1d));
        emp.put(2, new Employer("Roman1", 2d));

        awaitPartitionMapExchange();

        // Ensure the cache SQL is OK.
        assertQuery("SELECT * from emp WHERE SALARY >= 2")
            .returns("Roman1", 2d)
            .check();

        logChecker0.check(getTestTimeout());

        assertThrows("SELECT * FROM tbl_fun_dupl_col_nm", SqlValidatorException.class, "not found");

        assertThrows("SELECT * FROM tbl_fun_wrong_col_nm_num", SqlValidatorException.class, "not found");

        assertThrows("SELECT * FROM tbl_fun_wrong_ret_type_1", SqlValidatorException.class, "not found");
        assertThrows("SELECT * FROM tbl_fun_wrong_ret_type_2", SqlValidatorException.class, "not found");

        assertThrows("SELECT * FROM tbl_fun_wrong_col_types", SqlValidatorException.class, "not found");
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertThrows(String sql) {
        GridTestUtils.assertThrowsWithCause(() -> assertQuery(sql).check(), IgniteSQLException.class);
    }

    /** */
    public static class TableFunctions {
        /** */
        @QuerySqlFunction(tableColumnTypes = {int.class, int.class, int.class})
        public static Iterable<Collection<?>> tbl_fun_it(int x) {
            return Arrays.asList(
                Arrays.asList(x + 1, x + 2, x + 3),
                Arrays.asList(x + 4, x + 5, x + 6),
                Arrays.asList(x + 7, x + 8, x + 9)
            );
        }

        /** */
        @QuerySqlFunction(tableColumnTypes = {int.class, int.class, int.class})
        public static Iterable<Object[]> tbl_fun_arr(int x) {
            return Arrays.asList(
                new Object[] {x + 1, x + 2, x + 3},
                new Object[] {x + 4, x + 5, x + 6},
                new Object[] {x + 7, x + 8, x + 9}
            );
        }

        /** */
        @QuerySqlFunction(tableColumnTypes = {int.class, int.class, int.class})
        public static Collection<?> tbl_fun_arr_and_it(int x) {
            return Arrays.asList(
                new Object[] {x + 1, x + 2, x + 3},
                Arrays.asList(x + 4, x + 5, x + 6),
                new Object[] {x + 7, x + 8, x + 9}
            );
        }

        /** */
        @QuerySqlFunction(tableColumnTypes = {Integer.class, int.class, Double.class, double.class})
        public static Collection<List<?>> tbl_fun_boxing(int i1, Integer i2, double d1, Double d2) {
            return List.of(Arrays.asList(i1, i2, d1, d2));
        }

        /** */
        @QuerySqlFunction(tableColumnTypes = {Integer.class, String.class}, tableColumnNames = {"INT_COL", "STR_COL"})
        public static Iterable<?> tbl_fun_col_names(int i) {
            return Arrays.asList(
                Arrays.asList(i, "" + i),
                Arrays.asList(i * 10, "empty")
            );
        }
    }

    /** */
    public static class IncorrectTableFunctions {
        /** Duplicated column names. */
        @QuerySqlFunction(tableColumnTypes = {Integer.class, String.class}, tableColumnNames = {"INT_COL", "INT_COL"})
        public static Iterable<?> tbl_fun_dupl_col_nm(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }

        /** Non-matching number of the column names. */
        @QuerySqlFunction(tableColumnTypes = {Integer.class, String.class}, tableColumnNames = {"INT_COL"})
        public static Iterable<?> tbl_fun_wrong_col_nm_num(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }

        /** Wrong return type 1. */
        @QuerySqlFunction(tableColumnTypes = {Integer.class, Integer.class})
        public static Object[] tbl_fun_wrong_ret_type_1(int i) {
            return new Object[] {
                Arrays.asList(i * 2, i * 2 + 1),
                Arrays.asList(i * 3, i * 3 + 1)
            };
        }

        /** Wrong return type 2. */
        @QuerySqlFunction(tableColumnTypes = {Integer.class})
        public static void tbl_fun_wrong_ret_type_2(int i) {
            System.err.println("Test value: " + i);
        }

        /** Empty column types. */
        @QuerySqlFunction(tableColumnNames = {"INT_COL", "STR_COL"})
        public static Collection<?> tbl_fun_wrong_col_types(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }
    }

    /** */
    public static class AddFunctionsLibrary {
        /** */
        @QuerySqlFunction
        public static double add(double a, double b) {
            return a + b;
        }

        /** */
        @QuerySqlFunction
        public static int add(int a, int b, int c) {
            return a + b + c;
        }

        /** */
        @QuerySqlFunction(alias = "add")
        public static int addFour(int a, int b, int c, int d) {
            return a + b + c + d;
        }
    }

    /** */
    public static class MulFunctionsLibrary {
        /** */
        @QuerySqlFunction
        public static int mul(int a, int b) {
            return a * b;
        }

        /** */
        @QuerySqlFunction
        public static int mul(int a, int b, int c) {
            return a * b * c;
        }

        /** */
        @QuerySqlFunction
        public static boolean test(int a, int b, int res) {
            return a * b == res;
        }
    }

    /** */
    public static class OtherFunctionsLibrary {
        /** */
        @QuerySqlFunction
        public static double sq(double a) {
            return a * a;
        }

        /** */
        @QuerySqlFunction
        public static String echo(String s) {
            return s;
        }
    }
}
