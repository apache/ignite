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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlTableFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR;

/**
 * Integration test for user defined functions.
 */
@WithSystemProperty(key = IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR, value = "true")
public class UserDefinedFunctionsIntegrationTest extends AbstractBasicIntegrationTest {
    /** Log listener. */
    private static final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        if (igniteInstanceName.endsWith("0"))
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** */
    @Test
    public void testOverrideSystemFunction() throws Exception {
        // Add custom function with the same names in a custom schema.
        client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("TEST_CACHE_OWN")
            .setSqlSchema("OWN_SCHEMA")
            .setSqlFunctionClasses(OverrideSystemFunctionLibrary.class)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp_own")))
        );

        // Make sure those function have not affected 'PUBLIC' schema.
        assertQuery("SELECT UPPER(?)").withParams("abc").returns("ABC").check();
        assertQuery("select UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200L).check();
        assertQuery("select TYPEOF(?)").withParams(1L).returns("BIGINT").check();
        assertThrows("select PLUS(?, ?)", SqlValidatorException.class, "No match found for function signature", 1, 2);
        assertQuery("select ? + ?").withParams(1, 2).returns(3).check();

        // Ensure that new functions are successfully  created in the custom schema.
        assertQuery("SELECT \"OWN_SCHEMA\".UPPER(?)").withParams("abc").returns(3).check();
        assertQuery("select \"OWN_SCHEMA\".UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00')").returns(1).check();
        assertQuery("select \"OWN_SCHEMA\".TYPEOF('ABC')").returns(1).check();
        assertQuery("select \"OWN_SCHEMA\".PLUS(?, ?)").withParams(1, 2).returns(100).check();

        GridTestUtils.assertThrows(
            null,
            () ->
                client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("TEST_CACHE_PUB")
                    .setSqlFunctionClasses(OverrideSystemFunctionLibrary.class)
                    .setSqlSchema(QueryUtils.DFLT_SCHEMA)
                    .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp_pub")))),
            IgniteCheckedException.class,
            "already a standard function with the same name"
        );

        // Make sure that standard functions work once again.
        assertQuery("SELECT UPPER(?)").withParams("abc").returns("ABC").check();
        assertQuery("select UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200L).check();
        assertQuery("select TYPEOF(?)").withParams(1L).returns("BIGINT").check();

        // Make sure that operator + works and new function 'PLUS' also regustered in the default schema.
        assertQuery("select ? + ?").withParams(1, 2).returns(3).check();
        assertQuery("select PLUS(?, ?)").withParams(1, 2).returns(100);

        CalciteQueryProcessor qryProc = Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
        Map<String, IgniteSchema> schemas = GridTestUtils.getFieldValue(qryProc, "schemaHolder", "igniteSchemas");
        IgniteSchema dfltSchema = schemas.get(QueryUtils.DFLT_SCHEMA);

        assertEquals(0, dfltSchema.getFunctions("UPPER").size());
        assertEquals(0, dfltSchema.getFunctions("UNIX_SECONDS").size());
        assertEquals(0, dfltSchema.getFunctions("TYPEOF").size());
        assertEquals(1, dfltSchema.getFunctions("PLUS").size());
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
            .setSqlFunctionClasses(TableFunctionsLibrary.class)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp")))
        );

        emp.put(1, new Employer("Igor1", 1d));
        emp.put(2, new Employer("Roman1", 2d));

        awaitPartitionMapExchange();

        assertQuery("SELECT * from collectionRow(?)").withParams(1)
            .returns(2, 3, 4)
            .returns(5, 6, 7)
            .returns(8, 9, 10)
            .check();

        assertQuery("SELECT * from collectionRow(?) WHERE COL_2=4").withParams(2)
            .returns(3, 4, 5)
            .check();

        assertQuery("SELECT COL_1, COL_3 from collectionRow(?) WHERE COL_2=3").withParams(1)
            .returns(2, 4)
            .check();

        // Overrides.
        assertQuery("SELECT * from collectionRow(?, 2, ?)").withParams(1, 3)
            .returns(11, 22, 33)
            .returns(41, 52, 63)
            .returns(71, 82, 93)
            .check();

        assertQuery("SELECT * from arrayRow(?)").withParams(1)
            .returns(2, 3, 4)
            .returns(5, 6, 7)
            .returns(8, 9, 10)
            .check();

        assertQuery("SELECT COL_1, COL_3 from arrayRow_and_it(1) WHERE COL_1>4 AND COL_2>? AND COL_3>6").withParams(5)
            .returns(5, 7)
            .returns(8, 10)
            .check();

        assertQuery("SELECT * from boxingUnboxing(1, ?, ?, 4.0::FLOAT)").withParams(1, 4.0d)
            .returns(1, 1, 4.0d, 4.0d)
            .check();

        assertQuery("SELECT * from boxingUnboxing(1, 1, 2, 2)")
            .returns(1, 1, 2.0d, 2.0d)
            .check();

        assertQuery("SELECT * from boxingUnboxing(?, ?, ?, ?)").withParams(1, 1, 2.0d, 2.0d)
            .returns(1, 1, 2.0d, 2.0d)
            .check();

        assertQuery("SELECT * from emp WHERE SALARY >= (SELECT COL_1 from collectionRow(1) WHERE COL_2=3)")
            .returns("Roman1", 2d)
            .check();

        assertQuery("SELECT * from aliasedName(?)").withParams(1)
            .returns(2, 3, 4)
            .returns(5, 6, 7)
            .check();

        assertQuery("SELECT * from raiseException(?, ?, ?)").withParams(1, "test", false)
            .returns(2, "test2")
            .returns(3, "test3")
            .check();

        assertThrows("SELECT * from raiseException(?, ?, ?)", IgniteSQLException.class, "An error occurred while query executing",
            1, "test", true);

        assertThrows("SELECT * from raiseException(?, ?, ?)", RuntimeException.class, "Test exception",
            1, "test", true);

        // Object type.
        assertQuery("SELECT * from withObjectType(1)")
            .returns(1, new Employer("emp1", 1000d))
            .returns(10, new Employer("emp10", 10000d))
            .check();

        assertQuery("SELECT * from withObjectType(1) where EMP=?")
            .withParams(new Employer("emp10", 10000d))
            .returns(10, new Employer("emp10", 10000d))
            .check();
    }

    /** */
    @Test
    public void testIncorrectTableFunctions() throws Exception {
        LogListener logChecker0 = LogListener.matches("One or more column names is not unique")
            .andMatches("must match the number of column types")
            .andMatches("The method is expected to return a collection (iterable)")
            .andMatches("Column types cannot be empty")
            .andMatches("Column names cannot be empty")
            .build();

        listeningLog.registerListener(logChecker0);

        IgniteCache<Integer, Employer> emp = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp")
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(IncorrectTableFunctionsLibrary.class)
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp")))
        );

        emp.put(1, new Employer("Igor1", 1d));
        emp.put(2, new Employer("Roman1", 2d));

        awaitPartitionMapExchange();

        // Ensure the cache SQL is OK.
        assertQuery("SELECT * from emp WHERE SALARY >= 2")
            .returns("Roman1", 2d)
            .check();

        assertThrows("SELECT * FROM wrongRowLength(1)", IgniteSQLException.class,
            "row length [2] doesn't match defined columns number [3]");

        assertThrows("SELECT * FROM wrongRowType(1)", IgniteSQLException.class,
            "row type is neither Collection or Object[]");

        assertTrue(logChecker0.check(getTestTimeout()));

        String errTxt = "No match found for function signature";

        assertThrows("SELECT * FROM duplicateColumnName(1, 'a')", SqlValidatorException.class, errTxt);

        assertThrows("SELECT * FROM wrongColumnNamesNumber(1, 'a')", SqlValidatorException.class, errTxt);

        assertThrows("SELECT * FROM wrongReturnType(1, 'a')", SqlValidatorException.class, errTxt);

        assertThrows("SELECT * FROM noReturnType(1, 'a')", SqlValidatorException.class, errTxt);

        assertThrows("SELECT * FROM noColumnTypes(1, 'a')", SqlValidatorException.class, errTxt);

        assertThrows("SELECT * FROM noColumnNames(1, 'a')", SqlValidatorException.class, errTxt);
    }

    /** */
    @Test
    public void testUnregistrableTableFunctions() {
        GridTestUtils.assertThrowsAnyCause(
            null,
            () -> client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp")
                .setSqlFunctionClasses(UnregistrableTableFunctionsLibrary.class)),
            IgniteCheckedException.class,
            "both table and non-table function variants are defined"
        );
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertThrows(String sql) {
        GridTestUtils.assertThrowsWithCause(() -> assertQuery(sql).check(), IgniteSQLException.class);
    }

    /** */
    public static final class TableFunctionsLibrary {
        /** */
        private TableFunctionsLibrary() {
            // No-op.
        }

        /** Trivial test. Returns collections as row holders. */
        @QuerySqlTableFunction(columnTypes = {int.class, int.class, int.class}, columnNames = {"COL_1", "COL_2", "COL_3"})
        public static Iterable<Collection<?>> collectionRow(int x) {
            return Arrays.asList(
                Arrays.asList(x + 1, x + 2, x + 3),
                Arrays.asList(x + 4, x + 5, x + 6),
                Arrays.asList(x + 7, x + 8, x + 9)
            );
        }

        /** Overrides. */
        @QuerySqlTableFunction(columnTypes = {int.class, int.class, int.class}, columnNames = {"COL_1", "COL_2", "COL_3"})
        public static Collection<Collection<?>> collectionRow(int x, int y, int z) {
            return Arrays.asList(
                Arrays.asList(x + 10, y + 20, z + 30),
                Arrays.asList(x + 40, y + 50, z + 60),
                Arrays.asList(x + 70, y + 80, z + 90)
            );
        }

        /** Returns arrays as row holders. */
        @QuerySqlTableFunction(columnTypes = {int.class, int.class, int.class}, columnNames = {"COL_1", "COL_2", "COL_3"})
        public static Iterable<Object[]> arrayRow(int x) {
            return Arrays.asList(
                new Object[] {x + 1, x + 2, x + 3},
                new Object[] {x + 4, x + 5, x + 6},
                new Object[] {x + 7, x + 8, x + 9}
            );
        }

        /** Returns mixed row holders. */
        @QuerySqlTableFunction(columnTypes = {int.class, int.class, int.class}, columnNames = {"COL_1", "COL_2", "COL_3"})
        public static Collection<?> arrayRow_and_it(int x) {
            return Arrays.asList(
                new Object[] {x + 1, x + 2, x + 3},
                Arrays.asList(x + 4, x + 5, x + 6),
                new Object[] {x + 7, x + 8, x + 9}
            );
        }

        /** Boxed/unboxed test. */
        @QuerySqlTableFunction(columnTypes = {Integer.class, int.class, Double.class, double.class},
            columnNames = {"COL_1", "COL_2", "COL_3", "COL_4"})
        public static Collection<List<?>> boxingUnboxing(int i1, Integer i2, double d1, Double d2) {
            return List.of(Arrays.asList(i1, i2, d1, d2));
        }

        /** Alias test. */
        @QuerySqlTableFunction(columnTypes = {int.class, int.class, int.class}, columnNames = {"COL_1", "COL_2", "COL_3"},
            alias = "aliasedName")
        public static Iterable<Collection<?>> alias(int x) {
            return Arrays.asList(
                Arrays.asList(x + 1, x + 2, x + 3),
                Arrays.asList(x + 4, x + 5, x + 6)
            );
        }

        /** Can raise a user-side exception. */
        @QuerySqlTableFunction(columnTypes = {int.class, String.class}, columnNames = {"COL_1", "COL_2"})
        public static Iterable<Collection<?>> raiseException(int i, String str, boolean doThrow) {
            if (doThrow)
                throw new RuntimeException("Test exception.");

            return Arrays.asList(
                Arrays.asList(i + 1, str + (i + 1)),
                Arrays.asList(i + 2, str + (i + 2))
            );
        }

        /** User exception test. */
        @QuerySqlTableFunction(columnTypes = {int.class, Object.class}, columnNames = {"ID", "EMP"})
        public static Iterable<Collection<?>> withObjectType(int i) {
            return Arrays.asList(
                Arrays.asList(i, new Employer("emp" + i, i * 1000d)),
                Arrays.asList(i * 10, new Employer("emp" + i * 10, i * 10000d))
            );
        }
    }

    /** */
    public static final class IncorrectTableFunctionsLibrary {
        /** */
        private IncorrectTableFunctionsLibrary() {
            // No-op.
        }

        /** Duplicated column names. */
        @QuerySqlTableFunction(columnTypes = {Integer.class, String.class}, columnNames = {"INT_COL", "INT_COL"})
        public static Iterable<?> duplicateColumnName(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }

        /** Non-matching number of the column names. */
        @QuerySqlTableFunction(columnTypes = {Integer.class, String.class}, columnNames = {"INT_COL"})
        public static Iterable<?> wrongColumnNamesNumber(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }

        /** Wrong return type 1. */
        @QuerySqlTableFunction(columnTypes = {Integer.class, Integer.class}, columnNames = {"COL_1", "COL_2"})
        public static Object[] wrongReturnType(int i) {
            return new Object[] {
                Arrays.asList(i * 2, i * 2 + 1),
                Arrays.asList(i * 3, i * 3 + 1)
            };
        }

        /** Wrong return type 2. */
        @QuerySqlTableFunction(columnTypes = {Integer.class}, columnNames = {"COL_1"})
        public static void noReturnType(int i) {
            System.err.println("Test value: " + i);
        }

        /** Empty column types. */
        @QuerySqlTableFunction(columnTypes = {}, columnNames = {"INT_COL", "STR_COL"})
        public static Collection<?> noColumnTypes(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }

        /** Empty column names. */
        @QuerySqlTableFunction(columnTypes = {Integer.class, String.class}, columnNames = {})
        public static Collection<?> noColumnNames(int i, String s) {
            return Arrays.asList(
                Arrays.asList(i, s + i),
                Arrays.asList(i * 10, s + (i * 10))
            );
        }

        /** Incorrect row length. */
        @QuerySqlTableFunction(columnTypes = {int.class, String.class, double.class}, columnNames = {"INT_COL", "STR_COL", "DBL_COL"})
        public static Collection<?> wrongRowLength(int i) {
            return Arrays.asList(
                Arrays.asList(i, "code_" + i, i * 1000.0d),
                Arrays.asList(i + 15, "code_" + (i + 15), (i + 15) * 1000.0d),
                // Mismatched length.
                Arrays.asList(i * 100, i * 100000.0d)
            );
        }

        /** Incorrect row type. */
        @QuerySqlTableFunction(columnTypes = {int.class, String.class, double.class}, columnNames = {"INT_COL", "STR_COL", "DBL_COL"})
        public static Collection<?> wrongRowType(int i) {
            return Arrays.asList(
                Arrays.asList(i, "code_" + i, i * 1000.0d),
                "wrongType",
                // Mismatched length.
                Arrays.asList(i * 100, i * 100000.0d)
            );
        }
    }

    /** */
    public static final class UnregistrableTableFunctionsLibrary {
        /** */
        private UnregistrableTableFunctionsLibrary() {
            // No-op.
        }

        /** Both variants. */
        @QuerySqlFunction
        @QuerySqlTableFunction(columnTypes = {Integer.class, String.class}, columnNames = {"INT_COL", "STR_COL"})
        public static Collection<?> bothVariants(int i, String s) {
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
    @Test
    public void testInnerSql() {
        IgniteCache<Integer, Employer> emp4 = client.getOrCreateCache(this.<Integer, Employer>cacheConfiguration()
            .setName("emp4")
            .setSqlFunctionClasses(InnerSqlFunctionsLibrary.class)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp4")))
        );

        for (int i = 0; i < 100; i++)
            put(client, emp4, i, new Employer("Name" + i, (double)i));

        assertQuery(grid(0), "SELECT sum(salary(?, _key)) FROM emp4")
            .withParams(grid(0).name())
            .returns(4950d)
            .check();
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

    /** */
    public static class OverrideSystemFunctionLibrary {
        /** Overwrites standard 'UPPER(VARCHAR)'. */
        @QuerySqlFunction
        public static int upper(String s) {
            return F.isEmpty(s) ? 0 : s.length();
        }

        /** Overwrites standard 'UNIX_SECONDS(Timestamp)'. */
        @QuerySqlFunction
        public static int unix_seconds(Timestamp ts) {
            return 1;
        }

        /** Overwrites Ignite's 'TYPEOF(Object)'. */
        @QuerySqlFunction
        public static int typeof(Object o) {
            return 1;
        }

        /** Same name as the of operator '+' which is not a function. */
        @QuerySqlFunction
        public static int plus(int x, int y) {
            return 100;
        }
    }

    /** */
    public static class InnerSqlFunctionsLibrary {
        /** */
        @QuerySqlFunction
        public static double salary(String ignite, int key) {
            return (double)Ignition.ignite(ignite)
                .cache("emp4")
                .query(new SqlFieldsQuery("SELECT salary FROM emp4 WHERE _key = ?").setArgs(key))
                .getAll().get(0).get(0);
        }
    }
}
