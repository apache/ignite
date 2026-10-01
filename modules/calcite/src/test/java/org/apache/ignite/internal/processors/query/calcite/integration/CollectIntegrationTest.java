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

import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractNode;
import org.junit.Test;

/**
 * Integration test for collect node.
 */
public class CollectIntegrationTest extends AbstractBasicIntegrationTest {
    /**
     * Tests that collect node correctly handles the case when downstream requests
     * limited number of rows, where collect must push one row and then
     * properly terminate downstream.
     */
    @Test
    public void testRequestLimitedRowsCountFromCollect() {
        sql("CREATE TABLE t(a INT)");

        sql("INSERT INTO t (a) VALUES (?)", 0);

        String sql = "SELECT /*+ CNL_JOIN */ ARRAY(SELECT a FROM t) FROM t LIMIT 1";

        assertQuery(sql).resultSize(1).check();

        /**
         * The data source size of (buffer size + 1) is used to ensure that multiple batches are needed
         * on right hand of CNLJ to process all input rows, in this case left hand is not requested
         * immediately after endLeft() call.
         */
        for (int i = 1; i < AbstractNode.IN_BUFFER_SIZE + 1; i++)
            sql("INSERT INTO t (a) VALUES (?)", i);

        assertQuery(sql).resultSize(1).check();
    }

    /** Tests multiset collection from distributed and correlated queries. */
    @Test
    public void testMultisetQueries() {
        sql("CREATE TABLE t(id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, NULL)");

        assertQuery("SELECT * FROM UNNEST(MULTISET(SELECT val FROM t))")
            .returns(10).returns(10).returns(20).returns(NULL_RESULT).check();
        assertQuery("SELECT * FROM UNNEST(MULTISET(SELECT id, val FROM t))")
            .returns(1, 10).returns(2, 10).returns(3, 20).returns(4, null).check();
        assertQuery("SELECT * FROM UNNEST(MULTISET[ROW(1), ROW(2)])")
            .returns(1).returns(2).check();
        assertQuery("SELECT * FROM UNNEST(MULTISET[ROW(1, 10), ROW(2, 20)])")
            .returns(1, 10).returns(2, 20).check();
        assertQuery("SELECT id, CARDINALITY(MULTISET(SELECT b.val FROM t b WHERE b.val = a.val)) FROM t a")
            .returns(1, 2).returns(2, 2).returns(3, 1).returns(4, 0).check();
        assertQuery("SELECT id, CARDINALITY(MULTISET[val, val]) FROM t")
            .returns(1, 2).returns(2, 2).returns(3, 2).returns(4, 2).check();
        assertQuery("SELECT CARDINALITY(MULTISET(SELECT val FROM t) MULTISET UNION ALL " +
            "MULTISET(SELECT val FROM t))").returns(8).check();
    }
}
