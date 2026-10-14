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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for recursive common table expressions.
 */
public class RecursiveCteIntegrationTest extends AbstractBasicIntegrationTest {
    /** Number of invocations of a non-deterministic function. */
    private static final AtomicInteger nonDeterministicCallCnt = new AtomicInteger();

    /** Explicit and inferred recursion must produce the same rows. */
    @Test
    public void testOptionalRecursiveKeyword() {
        for (String keyword : new String[] {"", "RECURSIVE "}) {
            assertQuery("WITH " + keyword + "seed(n) AS (SELECT 1), numbers(n) AS (" +
                "SELECT n FROM seed UNION ALL SELECT n + 1 FROM numbers WHERE n < 3), " +
                "result AS (SELECT * FROM numbers) SELECT * FROM result")
                .returns(1)
                .returns(2)
                .returns(3)
                .check();

            assertQuery("SELECT * FROM (WITH " + keyword + "\"Numbers\"(n) AS (" +
                "SELECT 1 UNION ALL SELECT x.n + 1 FROM \"Numbers\" x WHERE x.n < 3) " +
                "SELECT * FROM \"Numbers\")")
                .returns(1)
                .returns(2)
                .returns(3)
                .check();

            assertQuery("WITH " + keyword + "numbers(n) AS (" +
                "SELECT 1 UNION SELECT n + 1 FROM numbers WHERE n < 3) SELECT * FROM numbers")
                .returns(1)
                .returns(2)
                .returns(3)
                .check();
        }
    }

    /** Checks explicit and implicit recursion with ORDER BY inside and outside the CTE. */
    @Test
    public void testRecursiveCteWithOrderBy() {
        for (String keyword : new String[] {"", "RECURSIVE "}) {
            assertQuery("WITH " + keyword + "numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3 " +
                "ORDER BY n" +
                ") " +
                "SELECT n FROM numbers ORDER BY n")
                .ordered()
                .returns(1)
                .returns(2)
                .returns(3)
                .check();
        }
    }

    /** FETCH, LIMIT and OFFSET cannot be combined with sorting of the recursive UNION. */
    @Test
    public void testRecursiveCteOrderByWithRowLimitingIsRejected() {
        for (String keyword : new String[] {"", "RECURSIVE "}) {
            for (String limit : new String[] {"FETCH FIRST 2 ROWS ONLY", "LIMIT 2", "OFFSET 1 ROW"}) {
                assertThrows("WITH " + keyword + "numbers(n) AS (SELECT 1 UNION ALL " +
                    "SELECT n + 1 FROM numbers WHERE n < 3 ORDER BY n " + limit + ") SELECT n FROM numbers",
                    IgniteSQLException.class,
                    "Unsupported recursive CTE: ORDER BY with FETCH, LIMIT or OFFSET is not supported");
            }
        }
    }

    /** A non-recursive CTE retains sorting and row limiting even in a WITH RECURSIVE clause. */
    @Test
    public void testNonRecursiveCteOrderByWithRowLimiting() {
        for (String keyword : new String[] {"", "RECURSIVE "}) {
            assertQuery("WITH " + keyword + "numbers(n) AS (SELECT 1 AS n UNION ALL SELECT 3 UNION ALL " +
                "SELECT 2 ORDER BY n DESC FETCH FIRST 2 ROWS ONLY) SELECT n FROM numbers ORDER BY n")
                .ordered()
                .returns(2)
                .returns(3)
                .check();
        }
    }

    /** An inner recursive CTE hides an ordinary outer CTE's name during early recursion detection. */
    @Test
    public void testOrderByWithNestedCteShadowing() {
        assertQuery("WITH numbers(n) AS (SELECT 9 AS n UNION ALL " +
            "SELECT n FROM (WITH numbers(n) AS (SELECT 1 UNION ALL " +
            "SELECT n + 1 FROM numbers WHERE n < 3 ORDER BY n) SELECT n FROM numbers) " +
            "ORDER BY n DESC FETCH FIRST 2 ROWS ONLY) SELECT n FROM numbers ORDER BY n")
            .ordered()
            .returns(3)
            .returns(9)
            .check();
    }

    /** Row limiting of a recursive CTE's consumer remains supported. */
    @Test
    public void testRecursiveCteOrderByWithOuterRowLimiting() {
        for (String keyword : new String[] {"", "RECURSIVE "}) {
            assertQuery("WITH " + keyword + "numbers(n) AS (SELECT 1 UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 5 ORDER BY n) " +
                "SELECT n FROM numbers ORDER BY n DESC FETCH FIRST 2 ROWS ONLY")
                .ordered()
                .returns(5)
                .returns(4)
                .check();
        }
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
    public void testRecursiveTermIsNotExecutedWhenSeedIsEmpty() {
        sql("CREATE TABLE empty_seed (n INT PRIMARY KEY)");

        assertQuery("WITH RECURSIVE numbers(n) AS (" +
            "SELECT n FROM empty_seed " +
            "UNION ALL " +
            "SELECT v.n FROM numbers RIGHT JOIN (VALUES (42)) v(n) ON TRUE" +
            ") " +
            "SELECT n FROM numbers FETCH FIRST 1 ROW ONLY")
            .resultSize(0)
            .check();
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
    public void testRecursiveTermWithMultipleSelfReferences() {
        assertQuery("WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT left_numbers.n + 1 " +
                "FROM numbers left_numbers " +
                "JOIN numbers right_numbers ON left_numbers.n = right_numbers.n " +
                "WHERE left_numbers.n < 3" +
            ") " +
            "SELECT n FROM numbers")
            .returns(1)
            .returns(2)
            .returns(3)
            .check();
    }

    /** */
    @Test
    public void testRecursiveCteWithMultipleJoinConsumers() {
        assertQuery("WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT l.n, r.n " +
            "FROM numbers l " +
            "JOIN numbers r ON l.n = r.n")
            .returns(1, 1)
            .returns(2, 2)
            .returns(3, 3)
            .check();
    }

    /** */
    @Test
    public void testRecursiveCteWithMultipleUnionConsumers() {
        assertQuery("WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers " +
            "UNION ALL " +
            "SELECT n FROM numbers")
            .returns(1)
            .returns(2)
            .returns(3)
            .returns(1)
            .returns(2)
            .returns(3)
            .check();
    }

    /** */
    @Test
    public void testSelfReferenceInScalarSubqueryIsRejected() {
        assertThrows(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT (SELECT n + 1 FROM numbers) FROM (VALUES (0))" +
            ") " +
            "SELECT n FROM numbers FETCH FIRST 3 ROWS ONLY",
            IgniteSQLException.class,
            "self-references inside subqueries are not supported"
        );
    }

    /** */
    @Test
    public void testSelfReferenceInDerivedTableIsRejected() {
        assertThrows(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM (SELECT n FROM numbers) WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers",
            IgniteSQLException.class,
            "self-references inside subqueries are not supported"
        );
    }

    /** */
    @Test
    public void testRecursiveCteCanBeReadFromScalarSubquery() {
        assertQuery(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT (SELECT MAX(n) FROM numbers)"
        )
            .returns(3)
            .check();
    }

    /** */
    @Test
    public void testRecursiveCteWithMultipleRecursiveBranches() {
        assertQuery("WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "(" +
                    "SELECT n + 1 FROM numbers WHERE n < 3 " +
                    "UNION ALL " +
                    "SELECT n + 10 FROM numbers WHERE n < 3" +
                ")" +
            ") " +
            "SELECT n FROM numbers")
            .returns(1)
            .returns(2)
            .returns(11)
            .returns(3)
            .returns(12)
            .check();
    }

    /** Both DISTINCT spellings eliminate duplicates in the seed and across recursive iterations. */
    @Test
    public void testRecursiveCteWithDistinctUnion() {
        for (String union : new String[] {"UNION", "UNION DISTINCT"}) {
            assertQuery("WITH RECURSIVE numbers(n) AS (" +
                "SELECT * FROM (VALUES (1), (1), (2)) " + union + " " +
                "SELECT MOD(n, 3) + 1 FROM numbers" +
                ") SELECT n FROM numbers")
                .returns(1)
                .returns(2)
                .returns(3)
                .check();
        }
    }

    /** NULLs compare equal and all columns participate in duplicate elimination. */
    @Test
    public void testRecursiveDistinctNulls() {
        assertQuery("WITH RECURSIVE numbers(n, label) AS (" +
            "SELECT * FROM (VALUES (1, CAST(NULL AS VARCHAR)), (1, CAST(NULL AS VARCHAR)), (1, 'x')) " +
            "UNION DISTINCT SELECT n, label FROM numbers" +
            ") SELECT n, label FROM numbers")
            .returns(1, null)
            .returns(1, "x")
            .check();
    }

    /** Duplicate-only input batches must keep requesting rows until the source ends. */
    @Test
    public void testRecursiveDistinctLargeDuplicateBatch() {
        assertQuery("WITH RECURSIVE numbers(n) AS (" +
            "SELECT 1 UNION SELECT n FROM numbers CROSS JOIN TABLE(SYSTEM_RANGE(1, 10000))" +
            ") SELECT n FROM numbers")
            .returns(1)
            .check();
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
    public void testNestedRecursiveCteStatesAreIsolated() {
        assertQuery("WITH RECURSIVE first_numbers(n) AS (" +
                "SELECT 10 " +
                "UNION ALL " +
                "SELECT n + 1 FROM first_numbers WHERE n < 12" +
            "), second_numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT second_numbers.n + 1 " +
                "FROM second_numbers " +
                "JOIN first_numbers ON second_numbers.n + 9 = first_numbers.n " +
                "WHERE second_numbers.n < 3" +
            ") " +
            "SELECT n FROM second_numbers")
            .returns(1)
            .returns(2)
            .returns(3)
            .check();
    }

    /** */
    @Test
    public void testIndependentNonDeterministicSubtreeIsEvaluatedForEveryIteration() {
        registerRecursiveFunctions();

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

    /** Registers SQL functions used by recursive CTE tests. */
    private void registerRecursiveFunctions() {
        client.getOrCreateCache(new CacheConfiguration<Integer, Integer>("recursive_functions")
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(RecursiveFunctions.class));
    }
}
