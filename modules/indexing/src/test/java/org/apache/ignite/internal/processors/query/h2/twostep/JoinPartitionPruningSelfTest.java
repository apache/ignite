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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

/**
 * Tests for join partition pruning.
 */
@SuppressWarnings("deprecation")
public class JoinPartitionPruningSelfTest extends AbstractPartitionPruningBaseTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearIoState();
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

        executeSql("INSERT INTO t1 VALUES ('1', '1')");
        executeSql("INSERT INTO t2 VALUES ('1', '1', '1')");

        executeSql("INSERT INTO t1 VALUES ('2', '2')");
        executeSql("INSERT INTO t2 VALUES ('2', '2', '2')");

        executeSql("INSERT INTO t1 VALUES ('3', '3')");
        executeSql("INSERT INTO t2 VALUES ('3', '3', '3')");

        executeSql("INSERT INTO t1 VALUES ('4', '4')");
        executeSql("INSERT INTO t2 VALUES ('4', '4', '4')");

        executeSql("INSERT INTO t1 VALUES ('5', '5')");
        executeSql("INSERT INTO t2 VALUES ('5', '5', '5')");

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

        executeSql("INSERT INTO t1 VALUES ('1', '1')");
        executeSql("INSERT INTO t2 VALUES ('1', '1', '1')");

        executeSql("INSERT INTO t1 VALUES ('2', '2')");
        executeSql("INSERT INTO t2 VALUES ('2', '2', '2')");

        executeSql("INSERT INTO t1 VALUES ('3', '3')");
        executeSql("INSERT INTO t2 VALUES ('3', '3', '3')");

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
     * Test joins with different affinity functions.
     */
    @Test
    public void testJoinWithDifferentAffinityFunctions() {
        // Partition count.
        checkAffinityFunctions(
            cacheConfiguration(256, 1, false, false, false),
            cacheConfiguration(256, 1, false, false, false),
            true
        );

        checkAffinityFunctions(
            cacheConfiguration(1024, 1, false, false, false),
            cacheConfiguration(256, 1, false, false, false),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 1, false, false, false),
            cacheConfiguration(1024, 1, false, false, false),
            false
        );

        // Backups.
        checkAffinityFunctions(
            cacheConfiguration(256, 1, false, false, false),
            cacheConfiguration(256, 2, false, false, false),
            true
        );

        // Different affinity functions.
        checkAffinityFunctions(
            cacheConfiguration(256, 2, true, false, false),
            cacheConfiguration(256, 2, false, false, false),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, false, false),
            cacheConfiguration(256, 2, true, false, false),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 2, true, false, false),
            cacheConfiguration(256, 2, true, false, false),
            false
        );

        // Node filters.
        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, true, false),
            cacheConfiguration(256, 2, false, false, false),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, false, false),
            cacheConfiguration(256, 2, false, true, false),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, true, false),
            cacheConfiguration(256, 2, false, true, false),
            false
        );

        // With and without persistence.
        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, false, true),
            cacheConfiguration(256, 2, false, false, false),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, false, false),
            cacheConfiguration(256, 2, false, false, true),
            false
        );

        checkAffinityFunctions(
            cacheConfiguration(256, 2, false, false, true),
            cacheConfiguration(256, 2, false, false, true),
            true
        );
    }

    /**
     * @param ccfg1 Cache config 1.
     * @param ccfg2 Cache config 2.
     * @param compatible Compatible affinity function flag (false when affinity is incompatible).
     */
    @SuppressWarnings("unchecked")
    private void checkAffinityFunctions(CacheConfiguration ccfg1, CacheConfiguration ccfg2, boolean compatible) {
        // Destroy old caches.
        Ignite cli = client();

        cli.destroyCaches(cli.cacheNames());

        // Start new caches.
        ccfg1.setName("t1");
        ccfg2.setName("t2");

        QueryEntity entity1 = new QueryEntity(KeyClass1.class, ValueClass.class).setTableName("t1");
        QueryEntity entity2 = new QueryEntity(KeyClass2.class, ValueClass.class).setTableName("t2");

        ccfg1.setQueryEntities(Collections.singletonList(entity1));
        ccfg2.setQueryEntities(Collections.singletonList(entity2));

        ccfg1.setKeyConfiguration(new CacheKeyConfiguration(entity1.getKeyType(), "k1"));
        ccfg2.setKeyConfiguration(new CacheKeyConfiguration(entity2.getKeyType(), "ak2"));

        ccfg1.setSqlSchema(QueryUtils.DFLT_SCHEMA);
        ccfg2.setSqlSchema(QueryUtils.DFLT_SCHEMA);

        client().createCache(ccfg1);
        client().createCache(ccfg2);

        // Conduct tests.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ?",
            (res) -> assertPartitions(
                partition("t1", "1")
            ),
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t2.ak2 = ?",
            (res) -> assertPartitions(
                partition("t2", "2")
            ),
            "2"
        );

        if (compatible) {
            execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
                (res) -> assertPartitions(
                    partition("t1", "1"),
                    partition("t2", "2")
                ),
                "1", "2"
            );
        }
        else {
            execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.ak2 WHERE t1.k1 = ? OR t2.ak2 = ?",
                (res) -> assertNoPartitions(),
                "1", "2"
            );
        }
    }

    /**
     * Create custom cache configuration.
     *
     * @param parts Partitions.
     * @param backups Backups.
     * @param customAffinity Custom affinity function flag.
     * @param nodeFilter Whether to set node filter.
     * @param persistent Whether to enable persistence.
     * @return Cache configuration.
     */
    @SuppressWarnings("IfMayBeConditional")
    private static CacheConfiguration cacheConfiguration(
        int parts,
        int backups,
        boolean customAffinity,
        boolean nodeFilter,
        boolean persistent
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(backups);

        RendezvousAffinityFunction affFunc;

        if (customAffinity)
            affFunc = new CustomRendezvousAffinityFunction();
        else
            affFunc = new RendezvousAffinityFunction();

        affFunc.setPartitions(parts);

        ccfg.setAffinity(affFunc);

        if (nodeFilter)
            ccfg.setNodeFilter(new CustomNodeFilter());

        if (persistent)
            ccfg.setDataRegionName(REGION_DISK);

        return ccfg;
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
            (res) -> assertNoPartitions(),
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

    /**
     * Custom affinity function.
     */
    private static class CustomRendezvousAffinityFunction extends RendezvousAffinityFunction {
        // No-op.
    }

    /**
     * Custom node filter.
     */
    private static class CustomNodeFilter implements IgnitePredicate<ClusterNode> {
        @Override public boolean apply(ClusterNode clusterNode) {
            return true;
        }
    }

    /**
     * Key class 1.
     */
    @SuppressWarnings("unused")
    private static class KeyClass1 {
        /** Key. */
        @QuerySqlField
        private String k1;
    }

    /**
     * Key class 2.
     */
    @SuppressWarnings("unused")
    private static class KeyClass2 {
        /** Key. */
        @QuerySqlField
        private String k1;

        /** Affinity key. */
        @QuerySqlField
        private String ak2;
    }

    /**
     * Value class.
     */
    @SuppressWarnings("unused")
    private static class ValueClass {
        /** Value. */
        @QuerySqlField
        private String v;
    }
}
