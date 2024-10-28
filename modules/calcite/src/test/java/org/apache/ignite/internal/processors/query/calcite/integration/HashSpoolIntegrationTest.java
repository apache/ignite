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
 * Hash spool test.
 */
public class HashSpoolIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Test
    public void testNullsInSearchRow() {
        executeSql("CREATE TABLE t(i1 INTEGER, i2 INTEGER) WITH " + atomicity());
        executeSql("INSERT INTO t VALUES (null, 0), (1, 1), (2, 2), (3, null)");

        assertQuery("SELECT i1, (SELECT i2 FROM t WHERE i1=t1.i1) FROM t t1")
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .returns(null, null)
            .returns(1, 1)
            .returns(2, 2)
            .returns(3, null)
            .check();

        assertQuery("SELECT (SELECT i1 FROM t WHERE i2=t1.i2), i2 FROM t t1")
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .returns(null, 0)
            .returns(1, 1)
            .returns(2, 2)
            .returns(null, null)
            .check();
    }

    /** */
    @Test
    public void testNullsInSearchRowMultipleColumns() {
        executeSql("CREATE TABLE t0(i1 INTEGER, i2 INTEGER) WITH " + atomicity());
        executeSql("CREATE TABLE t1(i1 INTEGER, i2 INTEGER) WITH " + atomicity());
        executeSql("INSERT INTO t0 VALUES (null, 0), (1, null), (null, 2), (3, null), (1, 1)");
        executeSql("INSERT INTO t1 VALUES (null, 0), (null, 1), (2, null), (3, null), (1, 1)");

        String sql = "SELECT /*+ CNL_JOIN, DISABLE_RULE('FilterSpoolMergeToSortedIndexSpoolRule') */ * " +
            "FROM t0 JOIN t1 ON t0.i1=t1.i1 AND t0.i2=t1.i2";

        assertQuery(sql)
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .returns(1, 1, 1, 1)
            .check();
    }

    /** */
    @Test
    public void testHashSpoolCondition() {
        executeSql("CREATE TABLE t(i INTEGER) WITH " + atomicity());
        executeSql("INSERT INTO t VALUES (0), (1), (2)");

        String sql = "SELECT i, (SELECT i FROM t WHERE i=t1.i AND i-1=0) FROM t AS t1";

        assertQuery(sql)
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .returns(0, null)
            .returns(1, 1)
            .returns(2, null)
            .check();
    }

    /** */
    @Test
    public void testIsNotDistinctFrom() {
        executeSql("CREATE TABLE t1(i1 INTEGER, i2 INTEGER) WITH " + atomicity());
        executeSql("CREATE TABLE t2(i3 INTEGER, i4 INTEGER) WITH " + atomicity());

        executeSql("INSERT INTO t1 VALUES (1, null), (2, 2), (null, 3), (3, null)");
        executeSql("INSERT INTO t2 VALUES (1, 1), (2, 2), (null, 3), (4, null)");

        String sql = "SELECT /*+ CNL_JOIN */ i1, i4 FROM t1 JOIN t2 ON i1 IS NOT DISTINCT FROM i3";

        assertQuery(sql)
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .returns(1, 1)
            .returns(2, 2)
            .returns(null, 3)
            .check();

        sql = "SELECT /*+ CNL_JOIN */ i1, i4 FROM t1 JOIN t2 ON i1 IS NOT DISTINCT FROM i3 AND i2 = i4";

        assertQuery(sql)
            .matches(QueryChecker.containsSubPlan("IgniteHashIndexSpool"))
            .returns(2, 2)
            .returns(null, 3)
            .check();
    }
}
