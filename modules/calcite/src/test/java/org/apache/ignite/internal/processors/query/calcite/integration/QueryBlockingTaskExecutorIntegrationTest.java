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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR;

/**
 * Integration test with common queries for query blocking task executor instead of striped task executor.
 */
@WithSystemProperty(key = IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR, value = "true")
public class QueryBlockingTaskExecutorIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Test
    public void testSimpleScan() {
        createAndPopulateTable();

        assertQuery("SELECT * FROM person").resultSize(5).check();
    }

    /** */
    @Test
    public void testJoinRehash() throws Exception {
        sql("CREATE TABLE order_items (id varchar, orderId int, amount int, PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=order_items,backups=1," + atomicity() + "\"");

        sql("CREATE TABLE orders (id int, region varchar, PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=orders,backups=1," + atomicity() + "\"");

        sql("CREATE INDEX order_items_orderId ON order_items (orderId ASC)");
        sql("CREATE INDEX orders_region ON orders (region ASC)");

        for (int i = 0; i < 500; i++) {
            sql("INSERT INTO orders VALUES(?, ?)", i, "region" + i % 10);
            for (int j = 0; j < 20; j++)
                sql("INSERT INTO order_items VALUES(?, ?, ?)", i + "_" + j, i, j);
        }

        String sql = "SELECT sum(i.amount)" +
            " FROM order_items i JOIN orders o ON o.id=i.orderId" +
            " WHERE o.region = ?";

        assertQuery(sql)
            .withParams("region0")
            .matches(QueryChecker.containsSubPlan("IgniteMergeJoin"))
            .matches(QueryChecker.containsSubPlan("IgniteExchange(distribution=[affinity"))
            .returns(9500L) // 50 * sum(0 .. 19)
            .check();

        // Create concurrent queries with a lot of tasks (join rehashing also add a concurrency,
        // since send batches between nodes)
        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++)
                assertQuery(sql).withParams("region" + (i % 10)).returns(9500L).check();
        }, 10, "query_starter");
    }
}
