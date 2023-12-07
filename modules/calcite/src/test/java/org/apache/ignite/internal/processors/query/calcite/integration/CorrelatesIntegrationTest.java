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

import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.Test;

/**
 * Tests correlated queries.
 */
public class CorrelatesIntegrationTest extends AbstractBasicIntegrationTest {
    /**
     * Checks correlates are assigned before access.
     */
    @Test
    public void testCorrelatesAssignedBeforeAccess() {
        sql("create table test_tbl(v INTEGER)");
        sql("INSERT INTO test_tbl VALUES (1)");

        assertQuery("SELECT t0.v, (SELECT t0.v + t1.v FROM test_tbl t1) AS j FROM test_tbl t0")
            .returns(1, 2)
            .check();
    }

    /**
     * Checks that correlates can't be moved under the table spool.
     */
    @Test
    public void testCorrelatesWithTableSpool() {
        sql("CREATE TABLE test(i1 INT, i2 INT)");
        sql("INSERT INTO test VALUES (1, 1), (2, 2)");

        assertQuery("SELECT (SELECT t1.i1 + t1.i2 + t0.i2 FROM test t1 WHERE i1 = 1) FROM test t0")
            .matches(QueryChecker.containsSubPlan("IgniteTableSpool"))
            .returns(3)
            .returns(4)
            .check();
    }

    /**
     * Tests resolving of collisions in correlates.
     */
    @Test
    public void testCorrelatesCollision() {
        sql("CREATE TABLE test1 (a INTEGER, b INTEGER)");
        sql("INSERT INTO test1 VALUES (11, 1), (12, 2), (13, 3)");
        sql("CREATE TABLE test2 (a INTEGER, c INTEGER)");
        sql("INSERT INTO test2 VALUES (11, 1), (12, 1), (13, 4)");

        // Collision by correlate variables in the left hand.
        assertQuery("SELECT * FROM test1 WHERE " +
            "EXISTS(SELECT * FROM test2 WHERE test1.a=test2.a AND test1.b<>test2.c) " +
            "AND NOT EXISTS(SELECT * FROM test2 WHERE test1.a=test2.a AND test1.b<test2.c)")
            .returns(12, 2)
            .check();

        // Collision by correlate variables in both, left and right hands.
        assertQuery("SELECT * FROM test1 WHERE " +
            "EXISTS(SELECT * FROM test2 WHERE (SELECT test1.a)=test2.a AND (SELECT test1.b)<>test2.c) " +
            "AND NOT EXISTS(SELECT * FROM test2 WHERE (SELECT test1.a)=test2.a AND (SELECT test1.b)<test2.c)")
            .returns(12, 2)
            .check();
    }

    /**
     * Tests colocated join possible with the help of correlated distribution.
     */
    @Test
    public void testCorrelatedDistribution() {
        sql("CREATE TABLE dept(deptid INTEGER, name VARCHAR, PRIMARY KEY(deptid))");
        sql("CREATE TABLE emp(empid INTEGER, deptid INTEGER, name VARCHAR, PRIMARY KEY(empid, deptid)) " +
            "WITH AFFINITY_KEY=deptid");

        sql("INSERT INTO dept VALUES (0, 'dept0'), (1, 'dept1'), (2, 'dept2')");
        sql("INSERT INTO emp VALUES (0, 0, 'emp0'), (1, 0, 'emp1'), (2, 0, 'emp2'), " +
            "(3, 2, 'emp3'), (4, 2, 'emp4'), (5, 3, 'emp5')");

        assertQuery("SELECT deptid, (SELECT COUNT(*) FROM emp WHERE emp.deptid = dept.deptid) FROM dept")
            .matches(QueryChecker.containsSubPlan("IgniteColocated"))
            .returns(0, 3L)
            .returns(1, 0L)
            .returns(2, 2L)
            .check();
    }
}
