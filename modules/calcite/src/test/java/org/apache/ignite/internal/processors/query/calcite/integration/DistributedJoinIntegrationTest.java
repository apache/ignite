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

import java.math.BigDecimal;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class DistributedJoinIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 3;
    }

    /** Test that resources (in particular inboxes) are cleaned up after executing join with rehashing. */
    @Test
    public void testRehashResourceCleanup() throws Exception {
        prepareTables();

        String sql = "SELECT sum(i.price * i.amount)" +
            " FROM order_items i JOIN orders o ON o.id=i.orderId" +
            " WHERE o.region = ?";

        assertQuery(sql)
            .withParams("region0")
            .matches(QueryChecker.containsSubPlan("IgniteMergeJoin"))
            .matches(QueryChecker.containsSubPlan("IgniteExchange(distribution=[affinity"))
            .returns(BigDecimal.valueOf(270))
            .check();

        // Here we only start queries and wait for result, actual resource clean up is checked by
        // AbstractBasicIntegrationTest.afterTest method.
        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++)
                sql(sql, "region" + (i % 10));
        }, 10, "query_starter");
    }

    /** Tests that null values are filtered out on rehashing. */
    @Test
    public void testRehashNullAffinityKeys() {
        prepareTables();

        // Add null values.
        for (int i = 0; i < 10; i++)
            sql("INSERT INTO order_items (id) VALUES(?)", "null_key_" + i);

        String sql = "SELECT sum(i.price * i.amount)" +
            " FROM order_items i JOIN orders o ON o.id=i.orderId" +
            " WHERE o.region = ?";

        assertQuery(sql)
            .withParams("region0")
            .matches(QueryChecker.containsSubPlan("IgniteExchange(distribution=[affinity"))
            .returns(BigDecimal.valueOf(270))
            .check();
    }

    /** */
    @Test
    public void testRehashOnRightHand() {
        prepareTables();

        assertQuery("SELECT /*+ ENFORCE_JOIN_ORDER */ o.id, i.id " +
            "FROM order_items oi JOIN items i ON (i.id = oi.itemId) JOIN orders o ON (oi.orderId = o.id) " +
            "WHERE oi.orderId between 2 and 3 and oi.itemId between 4 and 5")
            .matches(QueryChecker.containsSubPlan("IgniteExchange(distribution=[affinity"))
            .returns(2, 4).returns(2, 5).returns(3, 4).returns(3, 5)
            .check();
    }

    /** */
    @Test
    public void testTrimExchange() {
        sql("CREATE TABLE order_ids(id INTEGER PRIMARY KEY) WITH TEMPLATE=REPLICATED," + atomicity());
        prepareTables();

        sql("INSERT INTO order_ids(id) SELECT id FROM orders");

        assertQuery("SELECT sum(o.id) FROM orders o JOIN order_ids oid ON (o.id = oid.id)")
            .matches(QueryChecker.containsSubPlan("IgniteTrimExchange"))
            .returns(435L)
            .check();
    }

    /** */
    @Test
    public void testJoinWithBroadcastAggregate() {
        sql("CREATE TABLE items(id INT, name VARCHAR, PRIMARY KEY(id)) WITH " + atomicity());
        sql("CREATE TABLE selected_items_part(id INT, val VARCHAR) WITH " + atomicity());
        sql("CREATE TABLE selected_items_repl(id INT) WITH TEMPLATE=REPLICATED," + atomicity());

        for (int i = 0; i < 1000; i++)
            sql("INSERT INTO items VALUES (?, ?)", i, "item" + i);

        sql("INSERT INTO selected_items_part VALUES (10, 'val10'), (20, 'val20'), (30, 'val30')");
        sql("INSERT INTO selected_items_repl VALUES (10), (20), (30)");

        // Broadcast-distributed dynamic parameters.
        assertQuery("SELECT id, name FROM items WHERE id IN (SELECT * FROM (VALUES (?), (?), (?)))")
            .withParams(10, 20, 30)
            .returns(10, "item10").returns(20, "item20").returns(30, "item30")
            .check();

        // Single-distributed values from table.
        assertQuery("SELECT id, name FROM items WHERE id IN (SELECT avg(id) FROM selected_items_part GROUP BY val)")
            .returns(10, "item10").returns(20, "item20").returns(30, "item30")
            .check();

        // Broadcast-distributed values from table.
        assertQuery("SELECT id, name FROM items WHERE id IN (SELECT * FROM selected_items_repl)")
            .returns(10, "item10").returns(20, "item20").returns(30, "item30")
            .check();
    }

    /** Prepare tables orders and order_items with data. */
    private void prepareTables() {
        sql("CREATE TABLE items (\n" +
            "    id int,\n" +
            "    name varchar,\n" +
            "    PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=items,backups=1," + atomicity() + "\"");

        sql("CREATE TABLE order_items (\n" +
            "    id varchar,\n" +
            "    orderId int,\n" +
            "    itemId int,\n" +
            "    price decimal,\n" +
            "    amount int,\n" +
            "    PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=order_items,backups=1," + atomicity() + "\"");

        sql("CREATE TABLE orders (\n" +
            "    id int,\n" +
            "    region varchar,\n" +
            "    PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=orders,backups=1," + atomicity() + "\"");

        sql("CREATE INDEX order_items_orderId ON order_items (orderId ASC)");
        sql("CREATE INDEX order_items_itemId ON order_items (itemId ASC)");
        sql("CREATE INDEX orders_region ON orders (region ASC)");

        for (int i = 0; i < 200; i++)
            sql("INSERT INTO items VALUES(?, ?)", i, "item" + i);

        for (int i = 0; i < 30; i++) {
            sql("INSERT INTO orders VALUES(?, ?)", i, "region" + i % 10);
            for (int j = 0; j < 20; j++)
                sql("INSERT INTO order_items VALUES(?, ?, ?, ?, ?)", i + "_" + j, i, j, i / 10.0, j % 10);
        }
    }
}
