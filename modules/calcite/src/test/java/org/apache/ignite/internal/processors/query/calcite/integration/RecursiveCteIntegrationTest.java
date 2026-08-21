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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.Test;

/**
 * Integration tests for recursive common table expressions.
 */
public class RecursiveCteIntegrationTest extends AbstractBasicIntegrationTest {
    /** Number of invocations of a non-deterministic function. */
    private static final AtomicInteger nonDeterministicCallCnt = new AtomicInteger();

    /** Global memory quota for SQL queries. */
    private static final long GLOBAL_MEMORY_QUOTA = 10_000_000L;

    /** Query memory quota deliberately smaller than the largest recursive delta in the test below. */
    private static final long QUERY_MEMORY_QUOTA = 1_000_000L;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(
                new CalciteQueryEngineConfiguration()
                    .setGlobalMemoryQuota(GLOBAL_MEMORY_QUOTA)
                    .setQueryMemoryQuota(QUERY_MEMORY_QUOTA)));
    }

    /** */
    @Test
    public void testEmployeeHierarchy() {
        createEmployeeTable();

        assertQuery("WITH RECURSIVE employee_hierarchy (id, manager_id, name, depth) AS (" +
            "SELECT id, manager_id, name, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, e.name, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
            ") " +
            "SELECT id, manager_id, name, depth FROM employee_hierarchy ORDER BY depth, id")
            .returns(1, null, "CEO", 0)
            .returns(2, 1, "Manager", 1)
            .returns(4, 1, "Accountant", 1)
            .returns(3, 2, "Developer", 2)
            .check();
    }

    /** */
    @Test
    public void testRecursionStopsWhenDeltaIsEmpty() {
        sql("CREATE TABLE employee (id INT PRIMARY KEY, manager_id INT, name VARCHAR)");
        sql("INSERT INTO employee VALUES (1, NULL, 'CEO')");

        assertQuery("WITH RECURSIVE employee_hierarchy (id, manager_id, name, depth) AS (" +
            "SELECT id, manager_id, name, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, e.name, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
            ") " +
            "SELECT id, manager_id, name, depth FROM employee_hierarchy")
            .returns(1, null, "CEO", 0)
            .check();
    }

    /** */
    @Test
    public void testExplainRecursiveCte() {
        createEmployeeTable();

        String qry = "WITH RECURSIVE employee_hierarchy (id, manager_id, name, depth) AS (" +
            "SELECT id, manager_id, name, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, e.name, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
            ") " +
            "SELECT id, manager_id, name, depth FROM employee_hierarchy";

        String plan = (String)sql("EXPLAIN PLAN FOR " + qry).get(0).get(0);

        assertTrue(plan, plan.contains("IgniteRepeatUnion"));
        assertTrue(plan, plan.contains("IgniteRecursiveTableSpool"));
    }

    /** */
    @Test
    public void testRecursiveDeltaIsAccountedForMemoryQuota() {
        assertThrows(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 " +
                "FROM numbers " +
                "CROSS JOIN (VALUES (1), (2)) AS fanout(x) " +
                "WHERE n < 19" +
                ") " +
                "SELECT COUNT(*) FROM numbers",
            IgniteSQLException.class,
            "Query quota exceeded"
        );
    }

    /** */
    @Test
    public void testRecursiveTermWithoutSelfReferenceAfterOptimization() {
        assertQuery("WITH RECURSIVE numbers(n) AS (" +
            "SELECT 1 " +
            "UNION ALL " +
            "SELECT n + 1 FROM numbers WHERE FALSE" +
            ") " +
            "SELECT n FROM numbers")
            .returns(1)
            .check();
    }

    /** */
    @Test
    public void testRecursiveTermWithMultipleSelfReferencesIsRejected() {
        assertThrows(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT left_numbers.n + 1 " +
                "FROM numbers left_numbers " +
                "JOIN numbers right_numbers ON left_numbers.n = right_numbers.n " +
                "WHERE left_numbers.n < 3" +
            ") " +
            "SELECT n FROM numbers",
            IgniteSQLException.class,
            "the recursive term must contain no more than one self-reference"
        );
    }

    /** */
    @Test
    public void testRecursiveCteWithDistinctUnionIsRejected() {
        assertThrows(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers",
            IgniteSQLException.class,
            "only UNION ALL is supported"
        );
    }

    /** */
    @Test
    public void testStateIsIsolatedBetweenSameNamedRecursiveCtes() {
        assertQuery("SELECT /*+ MERGE_JOIN */ l.n, r.n " +
            "FROM (" +
                "WITH RECURSIVE numbers(n) AS (" +
                    "SELECT 1 " +
                    "UNION ALL " +
                    "SELECT n + 1 FROM numbers WHERE n < 3" +
                ") " +
                "SELECT n, n + 9 AS join_key FROM numbers" +
            ") l " +
            "JOIN (" +
                "WITH RECURSIVE numbers(n) AS (" +
                    "SELECT 10 " +
                    "UNION ALL " +
                    "SELECT n + 1 FROM numbers WHERE n < 12" +
                ") " +
                "SELECT n, n - 9 AS join_key FROM numbers" +
            ") r ON l.join_key = r.n")
            .matches(QueryChecker.containsSubPlan("IgniteMergeJoin"))
            .returns(1, 10)
            .returns(2, 11)
            .returns(3, 12)
            .check();
    }

    /** */
    @Test
    public void testIndependentNonDeterministicSubtreeIsEvaluatedForEveryIteration() {
        client.getOrCreateCache(new CacheConfiguration<Integer, Integer>("recursive_functions")
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(RecursiveFunctions.class));

        nonDeterministicCallCnt.set(0);

        String qry = "WITH RECURSIVE numbers(n, marker) AS (" +
            "SELECT 1, 0 " +
            "UNION ALL " +
            "SELECT n + 1, v.marker " +
            "FROM numbers " +
            "CROSS JOIN (SELECT nextRecursiveValue() AS marker) v " +
            "WHERE n < 4" +
            ") " +
            "SELECT n, marker FROM numbers ORDER BY n";

        assertQuery(qry)
            .returns(1, 0)
            .returns(2, 1)
            .returns(3, 2)
            .returns(4, 3)
            .check();
    }

    /** SQL functions used by recursive CTE tests. */
    public static class RecursiveFunctions {
        /** Returns a different value on every invocation. */
        @QuerySqlFunction(deterministic = false)
        public static int nextRecursiveValue() {
            return nonDeterministicCallCnt.incrementAndGet();
        }
    }

    /** */
    private void createEmployeeTable() {
        sql("CREATE TABLE employee (id INT PRIMARY KEY, manager_id INT, name VARCHAR)");

        sql("INSERT INTO employee VALUES " +
            "(1, NULL, 'CEO'), " +
            "(2, 1, 'Manager'), " +
            "(3, 2, 'Developer'), " +
            "(4, 1, 'Accountant')");
    }
}
