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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.junit.Test;

/**
 * Tests for use partition pruning at the SELECT step of the UPDATE/DELETE statements execution.
 */
@SuppressWarnings("deprecation")
public class MvccDmlPartitionPruningSelfTest extends AbstractPartitionPruningBaseTest {
    /** Rows count for test tables. */
    private static final int ROWS = 10;

    /** Recreate tables before each test statement. */
    private boolean recreateTables;

    /**
     * Test UPDATE statement.
     */
    @Test
    public void testUpdate() {
        recreateTables = false;

        recreateTables();

        // Key (not alias).
        execute("UPDATE t1 SET v1 = 'new1' WHERE k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1")
                );
                assertNodes(
                    node("t1", "1")
                );
                assertUpdatedRows(res, 1);
            },
            "1"
        );

        // Key (alias).
        execute("UPDATE t1 SET v1 = 'new2' WHERE _KEY = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "2")
                );
                assertNodes(
                    node("t1", "2")
                );
                assertUpdatedRows(res, 1);
            },
            "2"
        );

        // Non-affinity key.
        execute("UPDATE t2 SET v2 = 'new1' WHERE k2 = ?",
            (res) -> {
                assertNoPartitions();
                assertUpdatedRows(res, 1);
            },
            "1"
        );

        // Affinity key.
        execute("UPDATE t2 SET v2 = 'new1' WHERE ak2 = ?",
            (res) -> {
                assertPartitions(
                    partition("t2", "1")
                );
                assertNodes(
                    node("t2", "1")
                );
                assertUpdatedRows(res, 1);
            },
            "1"
        );

        // Expression: condition IN (...)
        execute("UPDATE t1 SET v1 = 'new1' WHERE k1 in (?, ?, ?)",
            (res) -> {
                assertPartitions(
                    partition("t1", "1"),
                    partition("t1", "2"),
                    partition("t1", "3")
                );
                assertNodes(
                    node("t1", "1"),
                    node("t1", "2"),
                    node("t1", "3")
                );
                assertUpdatedRows(res, 3);
            },
            "1", "2", "3"
        );

        // Expression: logical
        execute("UPDATE t1 SET v1 = 'new1' WHERE k1 in (?, ?) or k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1"),
                    partition("t1", "2"),
                    partition("t1", "3")
                );
                assertNodes(
                    node("t1", "1"),
                    node("t1", "2"),
                    node("t1", "3")
                );
                assertUpdatedRows(res, 3);
            },
            "3", "2", "1"
        );

        // No request (empty partitions).
        execute("UPDATE t1 SET v1 = 'new1' WHERE k1 in (?, ?) and k1 = ?",
            (res) -> {
                assertNoRequests();
                assertUpdatedRows(res, 0);
            },
            "3", "2", "1"
        );

        // Complex key.
        BinaryObject key = client().binary().builder("t2_key")
            .setField("k1", "5")
            .setField("ak2", "5")
            .build();

        List<List<?>> res = executeSingle("UPDATE t2 SET v2 = 'new1' WHERE _KEY = ?", key);
        assertPartitions(
            partition("t2", "5")
        );
        assertNodes(
            node("t2", "5")
        );
        assertUpdatedRows(res, 1);
    }

    /**
     * Test UPDATE statement.
     */
    @Test
    public void testDelete() {
        recreateTables = true;

        // Expression: condition IN (...)
        execute("DELETE FROM t1 WHERE k1 in (?, ?, ?)",
            (res) -> {
                assertPartitions(
                    partition("t1", "1"),
                    partition("t1", "2"),
                    partition("t1", "3")
                );
                assertNodes(
                    node("t1", "1"),
                    node("t1", "2"),
                    node("t1", "3")
                );
                assertUpdatedRows(res, 3);
            },
            "1", "2", "3"
        );

        // Expression: logical OR
        execute("DELETE FROM t1 WHERE k1 in (?, ?) or k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1"),
                    partition("t1", "2"),
                    partition("t1", "3")
                );
                assertNodes(
                    node("t1", "1"),
                    node("t1", "2"),
                    node("t1", "3")
                );
                assertUpdatedRows(res, 3);
            },
            "3", "2", "1"
        );

        // No request (empty partitions).
        execute("DELETE FROM t1  WHERE k1 in (?, ?) and k1 = ?",
            (res) -> {
                assertNoRequests();
                assertUpdatedRows(res, 0);
            },
            "3", "2", "1"
        );
    }

    /**
     * Drop, create and fill test tables.
     */
    private void recreateTables() {
        Ignite cli = client();

        cli.destroyCaches(cli.cacheNames());

        createPartitionedTable(true, "t1",
            pkColumn("k1"),
            "v1");

        createPartitionedTable(true, "t2",
            pkColumn("k2"),
            affinityColumn("ak2"),
            "v2");

        for (int i = 0; i < ROWS; ++i) {
            executeSql("INSERT INTO t1 VALUES (?, ?)",
                Integer.toString(i), Integer.toString(i));

            executeSql("INSERT INTO t2 VALUES (?, ?, ?)",
                Integer.toString(i), Integer.toString(i), Integer.toString(i));
        }
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> executeSingle(String sql, Object... args) {
        if (recreateTables)
            recreateTables();

        return super.executeSingle(sql, args);
    }

    /**
     * @param res Updated results.
     * @param expUpdated Expected updated rows count.
     */
    private static void assertUpdatedRows(List<List<?>> res, long expUpdated) {
        assertEquals(1, res.size());
        assertEquals(expUpdated, res.get(0).get(0));
    }
}
