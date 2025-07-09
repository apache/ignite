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
public class CorrelatesIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /**
     * Checks correlates are assigned before access.
     */
    @Test
    public void testCorrelatesAssignedBeforeAccess() {
        sql("create table test_tbl(v INTEGER) WITH " + atomicity());
        sql("INSERT INTO test_tbl VALUES (1)");

        assertQuery("SELECT t0.v, (SELECT t0.v + t1.v FROM test_tbl t1) AS j FROM test_tbl t0")
            .returns(1, 2)
            .check();
    }

    /**
     * Check compiled expression cache correctness for correlated variables with different data types.
     */
    @Test
    public void testCorrelatesDifferentDataType() {
        for (String type : new String[] {"INTEGER", "TINYINT"}) {
            try {
                sql("CREATE TABLE t1(v INTEGER) WITH " + atomicity());
                sql("CREATE TABLE t2(v " + type + ") WITH " + atomicity());
                sql("INSERT INTO t1 VALUES (1)");
                sql("INSERT INTO t2 VALUES (1)");

                assertQuery("SELECT (SELECT t1.v + t2.v FROM t1) FROM t2")
                    .returns(2)
                    .check();
            }
            finally {
                clearTransaction();

                sql("DROP TABLE t1");
                sql("DROP TABLE t2");
            }
        }
    }

    /**
     * Checks that correlates can't be moved under the table spool.
     */
    @Test
    public void testCorrelatesWithTableSpool() {
        sql("CREATE TABLE test(i1 INT, i2 INT) WITH " + atomicity());
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
        sql("CREATE TABLE test1 (a INTEGER, b INTEGER) WITH " + atomicity());
        sql("CREATE TABLE test2 (a INTEGER, c INTEGER) WITH " + atomicity());
        sql("INSERT INTO test1 VALUES (11, 1), (12, 2), (13, 3)");
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
        sql("CREATE TABLE dept(deptid INTEGER, name VARCHAR, PRIMARY KEY(deptid)) WITH " + atomicity());
        sql("CREATE TABLE emp(empid INTEGER, deptid INTEGER, name VARCHAR, PRIMARY KEY(empid, deptid)) " +
            "WITH AFFINITY_KEY=deptid," + atomicity());

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

    /** */
    @Test
    public void testTwoTablesCorrelatedSubquery() {
        sql("CREATE TABLE T1(ID INT, REF INT) WITH " + atomicity());
        sql("CREATE TABLE T2(ID INT, REF INT) WITH " + atomicity());
        sql("CREATE TABLE T3(ID1 INT, ID2 INT) WITH " + atomicity());

        sql("INSERT INTO T1 VALUES(1, 1)");
        sql("INSERT INTO T2 VALUES(1, 1)");
        sql("INSERT INTO T3 VALUES(1, 1)");

        assertQuery("SELECT T1.ID, T2.ID FROM T1 JOIN T2 ON (T1.ID = T2.ID) " +
            "WHERE EXISTS (SELECT 1 FROM T3 WHERE T3.ID1 = T1.REF AND T3.ID2 = T2.REF)").returns(1, 1).check();
    }

    /** */
    @Test
    public void testCorrelateInSecondFilterSubquery() {
        sql("CREATE TABLE T1(ID1 INT, REF11 INT, REF12 INT) WITH " + atomicity());
        sql("CREATE TABLE T2(ID2 INT, REF21 INT) WITH " + atomicity());
        sql("CREATE TABLE T3(ID3 INT, REF31 INT) WITH " + atomicity());

        sql("INSERT INTO T1 VALUES(1, 1, 1)");
        sql("INSERT INTO T1 VALUES(2, 2, 2)");
        sql("INSERT INTO T1 VALUES(2, 2, 1)");
        sql("INSERT INTO T1 VALUES(2, 1, 1)");
        sql("INSERT INTO T1 VALUES(3, 3, 3)");
        sql("INSERT INTO T1 VALUES(3, 3, 2)");
        sql("INSERT INTO T1 VALUES(3, 2, 2)");
        sql("INSERT INTO T1 VALUES(3, 2, 1)");
        sql("INSERT INTO T1 VALUES(3, 1, 1)");

        sql("INSERT INTO T2 VALUES(1, 1)");
        sql("INSERT INTO T2 VALUES(1, 2)");
        sql("INSERT INTO T2 VALUES(2, 1)");
        sql("INSERT INTO T2 VALUES(2, 2)");

        sql("INSERT INTO T3 VALUES(2, 1)");
        sql("INSERT INTO T3 VALUES(2, 2)");
        sql("INSERT INTO T3 VALUES(3, 3)");
        sql("INSERT INTO T3 VALUES(3, 2)");
        sql("INSERT INTO T3 VALUES(3, 1)");

        assertQuery("SELECT ID1 FROM T1 WHERE REF11 IN " +
            "(SELECT ID2 FROM T2 WHERE REF21 IN (SELECT REF31 FROM T3 WHERE ID3 = T1.ID1))")
            .returns(2).returns(3).check();
    }
}
