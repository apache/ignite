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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Integration test for user defined functions.
 */
public class UserDefinedFunctionsIntegrationTest extends AbstractBasicIntegrationTest {
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
