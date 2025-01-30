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

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
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
    public void testSameSignatureRegistered() throws Exception {
        LogListener logChecker = LogListener.matches("Unable to register function 'SAMESIGN'. Other function " +
            "with the same name and parameters is already registered").build();

        listeningLog.registerListener(logChecker);

        // Actually, we might use QuerySqlFunction#alias instead of declaring additional method holding class (OtherFunctionsLibrary2).
        // But Class#getDeclaredMethods() seems to give methods with a different order. If we define methods with one class,
        // we can get one 'sameSign' registered before another. And the test would become flaky.
        client.getOrCreateCache(new CacheConfiguration<Integer, Object>("emp")
            .setSqlFunctionClasses(OtherFunctionsLibrary.class, OtherFunctionsLibrary2.class));

        // Ensure that 1::INTEGER isn't returned by OtherFunctionsLibrary2#sameSign(int).
        assertQuery("SELECT \"emp\".sameSign(1)").returns("echo_1").check();

        // Ensure that OtherFunctionsLibrary#sameSign2(int) isn't registered.
        assertThrows("SELECT \"emp\".sameSign2(1)", SqlValidatorException.class,
            "No match found for function signature SAMESIGN2");

        logChecker.check(getTestTimeout());
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
    @SuppressWarnings("ThrowableNotThrown")
    private void assertThrows(String sql) {
        GridTestUtils.assertThrowsWithCause(() -> assertQuery(sql).check(), IgniteSQLException.class);
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

        /** The signature interferes with aliased {@link OtherFunctionsLibrary2#sameSign2(int)}. */
        @QuerySqlFunction
        public static String sameSign(int v) {
            return "echo_" + v;
        }
    }

    /** */
    public static class OtherFunctionsLibrary2 {
        /** The aliased signature interferes with {@link OtherFunctionsLibrary#sameSign(int)}. */
        @QuerySqlFunction(alias = "sameSign")
        public static int sameSign2(int v) {
            return v;
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
