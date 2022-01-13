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
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * Tests specific cases of partition pruning for tables created with QueryEntity API.
 */
public class JoinQueryEntityPartitionPruningSelfTest extends AbstractPartitionPruningBaseTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearIoState();
    }

    /** */
    @Parameterized.Parameters(name = "createWithSql = {0}")
    public static List<?> params() {
        return F.asList(false);
    }

    /**
     * Test custom aliases on PK.
     */
    @Test
    public void testPkAliasJoin() {
        createPartitionedTable("t1",
            pkColumn("k1", "ask1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1", "ask1"),
            "v2");

        fillTable("t1", 2, 5);
        fillTable("t2", 2, 5);

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.ask1 = t2.ask1 WHERE t1.ask1 = ?",
            (res) -> {
                assertPartitions(
                    partition("t1", "1")
                );
                assertEquals(1, res.size());
                assertEquals("1", res.get(0).get(0));
            },
            "1"
        );

        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.ask1 = t2.ask1 WHERE t2.ask1 = ?",
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
     * Test aliases on affinity key.
     */
    @Test
    public void testAffinityAliasJoin() {
        createPartitionedTable("t1",
            pkColumn("k1"),
            "v2");

        createPartitionedTable("t2",
            pkColumn("k1"),
            affinityColumn("ak2", "asak2"),
            "v3");

        fillTable("t1", 2, 5);
        fillTable("t2", 3, 5);

        // Affinity key.
        execute("SELECT * FROM t1 INNER JOIN t2 ON t1.k1 = t2.asak2 WHERE t2.asak2 = ?",
            (res) -> {
                assertPartitions(
                    partition("t2", "4")
                );
                assertEquals(1, res.size());
                assertEquals("4", res.get(0).get(0));
            },
            "4"
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
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return true;
        }
    }
}
