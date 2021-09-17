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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * Tests for join partition pruning.
 */
public class JoinPartitionPruningSelfTest extends AbstractPartitionPruningBaseTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearIoState();
    }

    /** */
    @Parameterized.Parameters(name = "createWithSql = {0}")
    public static List<?> params() {
        return F.asList(true, false);
    }

    /**
     * Test PK only join.
     */
    @Test
    public void testPkOnlyJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            "v2");

        fillTable("t1", 2, 5);
        fillTable("t2", 2, 5);

        // Key (not alias).
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        // Key (alias).
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t1._KEY = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        // Non-affinity key.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t2.k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "3")
                );
                assertEquals(1, res.size());
                assertEquals("3", res.get(0).get(0));
            },
            "3"
        );
    }

    /**
     * Test simple join.
     */
    @Test
    public void testSimpleJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        fillTable("t1", 2, 5);
        fillTable("t2", 3, 5);

        // Key (not alias).
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        // Key (alias).
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1._KEY = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        // Non-affinity key.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2.k1 = ?",
            (res) -> {
                assertNoPartitions();
                assertEquals(1, res.size());
                assertEquals("3", res.get(0).get(0));
            },
            "3"
        );

        // Affinity key.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2.ak2 = ?",
            (res) -> {
                assertPartitions(
                    partition("t2", "4")
                );
                assertEquals(1, res.size());
                assertEquals("4", res.get(0).get(0));
            },
            "4"
        );

        // Complex key.
        BinaryObject key = client().binary().builder("t2_key").setField("k1", "5").setField("ak2", "5").build();

        List<List<?>> res = executeSingle("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2._KEY = ?", key);
        assertPartitions(
            partition("t2", "5")
        );
        assertEquals(1, res.size());
        assertEquals("5", res.get(0).get(0));
    }

    /**
     * Test how partition ownership is transferred in various cases.
     */
    @Test
    public void testPartitionTransfer() {
        // First co-located table.
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2"
        );

        // Second co-located table.
        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3"
        );

        // Third co-located table.
        createPartitionedTable("t3",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3",
            "v4"
        );

        // Transfer through "AND".
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1", "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertNoRequests(),
            "1", "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1", "1", "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? AND t2.ak2 IN (?, ?)",
            (res) -> assertNoRequests(),
            "1", "2", "3"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 IN (?, ?) AND t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                partition("t1", "2")
            ),
            "1", "2", "2", "3"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 IN (?, ?) AND t2.ak2 IN (?, ?)",
            (res) -> assertNoRequests(),
            "1", "2", "3", "4"
        );

        // Transfer through "OR".
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1", "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t2", "2")
            ),
            "1", "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? OR t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t2", "2")
            ),
            "1", "1", "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? OR t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t2", "2"),
                partition("t2", "3")
            ),
            "1", "2", "3"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 IN (?, ?) OR t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t1", "2"),
                partition("t2", "3")
            ),
            "1", "2", "2", "3"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 IN (?, ?) OR t2.ak2 IN (?, ?)",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t1", "2"),
                partition("t2", "3"),
                partition("t2", "4")
            ),
            "1", "2", "3", "4"
        );

        // Multi-way co-located JOIN.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 INNER JOIN t3 ON t1.k1 = t3.ak2 " +
                "WHERE t1.k1 = ? AND t2.ak2 = ? AND t3.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1", "1", "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 INNER JOIN t3 ON t1.k1 = t3.ak2 " +
                "WHERE t1.k1 = ? AND t2.ak2 = ? AND t3.ak2 = ?",
            (res) -> assertNoRequests(),
            "1", "2", "3"
        );

        // No transfer through intermediate table.
        execute("SELECT * FROM t1 INNER JOIN t3 ON t1.k1 = t3.v3 INNER JOIN t2 ON t3.v4 = t2.ak2 " +
                "WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "1"
        );

        // No transfer through disjunction.
        execute("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t1.k1 = ? OR t1.k1 = t2.ak2",
            (res) -> assertNoPartitions(),
            "1"
        );
    }

    /**
     * Test cross-joins. They cannot "transfer" partitions between joined tables.
     */
    @Test
    public void testCrossJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        fillTable("t1", 2, 3);
        fillTable("t2", 3, 3);

        // Left table, should work.
        execute("SELECT * FROM t1, t2 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t1.k1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        // Right table, should work.
        execute("SELECT * FROM t1, t2 WHERE t2.ak2 = ?",
            (res) -> {
                assertPartitions(
                    partition("t2", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t2.ak2 = ?",
            (res) -> {
                assertPartitions(
                    partition("t2", "2")
                );
                assertEquals(1, res.size());
                assertEquals("2", res.get(0).get(0));
            },
            "2"
        );

        execute("SELECT * FROM t1, t2 WHERE t1.k1=? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "3", "3"
        );

        // Two tables, should not work.
        execute("SELECT * FROM t1, t2 WHERE t1.k1=? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "3", "3"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON 1=1 WHERE t1.k1=? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "3", "3"
        );
    }

    /**
     * Test non-equijoins.
     */
    @Test
    public void testThetaJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        // Greater than.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 > t2.ak2 WHERE t1.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 > t2.ak2 WHERE t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 > t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 > t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "2"
        );

        // Less than.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 < t2.ak2 WHERE t1.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 < t2.ak2 WHERE t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 < t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 < t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "2"
        );

        // Non-equal.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 <> t2.ak2 WHERE t1.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 <> t2.ak2 WHERE t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 <> t2.ak2 WHERE t1.k1 = ? AND t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 <> t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1", "2"
        );
    }

    /**
     * Test joins with REPLICATED cache.
     */
    @Test
    public void testJoinWithReplicated() {
        // First co-located table.
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2"
        );

        // Replicated table.
        createReplicatedTable("t2",
            pkColumn("k1"),
            "v2",
            "v3"
        );

        // Only partition from PARTITIONED cache should be used.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 = ? AND t2.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1", "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 IN (?, ?) AND t2.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t1", "2")
            ),
            "1", "2", "3"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t1.k1 = ? OR t2.k1 = ?",
            (res) -> assertNoPartitions(),
            "1", "2"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.k1 WHERE t2.k1 = ?",
            (res) -> assertNoPartitions(),
            "1"
        );
    }

    /**
     * Test joins with subqueries.
     */
    @Test
    public void testJoinWithSubquery() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        execute("SELECT * FROM t1 INNER JOIN (SELECT * FROM t2) T2_SUB ON t1.k1 = T2_SUB.ak2 WHERE t1.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN (SELECT * FROM t2) T2_SUB ON t1.k1 = T2_SUB.ak2 WHERE T2_SUB.ak2 = ?",
            (res) -> assertPartitions(
                partition("t2", "1")
            ),
            "1"
        );
    }

    /**
     * Test joins when explicit partitions are set.
     */
    @Test
    public void testExplicitPartitions() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        executeSqlFieldsQuery(new SqlFieldsQuery("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 " +
            "WHERE t1.k1=? OR t2.ak2=?").setArgs("1", "2").setPartitions(1));

        assertPartitions(1);
    }

    /**
     * Test outer joins.
     */
    @Test
    public void testOuterJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2"),
            "v3");

        execute("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2.ak2 = ?",
            (res) -> assertNoPartitions(),
            "1"
        );

        execute("SELECT * FROM t1 LEFT OUTER JOIN t2 T2_1 ON t1.k1 = T2_1.ak2 INNER JOIN t2 T2_2 ON T2_1.k1 = T2_2.k1 " +
                "WHERE T2_2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t2", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 LEFT OUTER JOIN t2 T2_1 ON t1.k1 = T2_1.ak2 INNER JOIN t2 T2_2 ON t1.k1 = T2_2.ak2 " +
                "WHERE T2_1.ak2 = ? AND T2_2.ak2=?",
            (res) -> assertPartitions(
                partition("t2", "2")
            ),
            "1", "2"
        );
    }

    /**
     * Test JOINs on a single table.
     */
    @Test
    public void testSelfJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        execute("SELECT * FROM t1 A INNER JOIN t1 B ON A.k1 = B.k1 WHERE A.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 A INNER JOIN t1 B ON A.k1 = B.k1 WHERE A.k1 = ? AND B.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1", "1"
        );

        execute("SELECT * FROM t1 A INNER JOIN t1 B ON A.k1 = B.k1 WHERE A.k1 = ? AND B.k1 = ?",
            (res) -> assertNoRequests(),
            "1", "2"
        );

        execute("SELECT * FROM t1 A INNER JOIN t1 B ON A.k1 = B.k1 WHERE A.k1 = ? OR B.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1"),
                partition("t1", "2")
            ),
            "1", "2"
        );
    }
}
