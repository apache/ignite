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

package org.apache.ignite.cache.affinity;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Base tests of {@link AffinityFunction} implementations with user provided backup filter.
 */
public abstract class AffinityFunctionBackupFilterAbstractSelfTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Split attribute name. */
    private static final String SPLIT_ATTRIBUTE_NAME = "split-attribute";

    /** Split attribute value. */
    private String splitAttrVal;

    /** Attribute value for first node group. */
    public static final String FIRST_NODE_GROUP = "A";

    /** Backup count. */
    private int backups = 1;

    /** Test backup filter. */
    protected static final IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter =
        new IgniteBiPredicate<ClusterNode, ClusterNode>() {
            @Override public boolean apply(ClusterNode primary, ClusterNode backup) {
                assert primary != null : "primary is null";
                assert backup != null : "backup is null";

                return !F.eq(primary.attribute(SPLIT_ATTRIBUTE_NAME), backup.attribute(SPLIT_ATTRIBUTE_NAME));
            }
        };

    /** Test backup filter. */
    protected static final IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter =
        new IgniteBiPredicate<ClusterNode, List<ClusterNode>>() {
            @Override public boolean apply(ClusterNode node, List<ClusterNode> assigned) {
                assert node != null : "primary is null";
                assert assigned != null : "backup is null";

                Map<String, Integer> backupAssignedAttribute = getAttributeStatistic(assigned);

                String nodeAttributeVal = node.attribute(SPLIT_ATTRIBUTE_NAME);

                if (FIRST_NODE_GROUP.equals(nodeAttributeVal)
                    && backupAssignedAttribute.get(FIRST_NODE_GROUP) < 2)
                    return true;

                return backupAssignedAttribute.get(nodeAttributeVal).equals(0);
            }
        };

    /**
     * @param nodes List of cluster nodes.
     * @return Statistic.
     */
    @NotNull private static Map<String, Integer> getAttributeStatistic(Collection<ClusterNode> nodes) {
        Map<String, Integer> backupAssignedAttribute = new HashMap<>();

        backupAssignedAttribute.put(FIRST_NODE_GROUP, 0);

        backupAssignedAttribute.put("B", 0);

        backupAssignedAttribute.put("C", 0);

        for (ClusterNode assignedNode: nodes) {
            if (assignedNode == null)
                continue;

            String val = assignedNode.attribute(SPLIT_ATTRIBUTE_NAME);

            Integer cnt = backupAssignedAttribute.get(val);

            backupAssignedAttribute.put(val, cnt + 1);
        }

        return backupAssignedAttribute;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(backups);

        if (backups < 2)
            cacheCfg.setAffinity(affinityFunction());
        else
            cacheCfg.setAffinity(affinityFunctionWithAffinityBackupFilter());

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(spi);
        cfg.setUserAttributes(F.asMap(SPLIT_ATTRIBUTE_NAME, splitAttrVal));

        return cfg;
    }

    /**
     * @return Affinity function for test.
     */
    protected abstract AffinityFunction affinityFunction();

    /**
     * @return Affinity function for test.
     */
    protected abstract AffinityFunction affinityFunctionWithAffinityBackupFilter();

    /**
     * @throws Exception If failed.
     */
    public void testPartitionDistribution() throws Exception {
        backups = 1;

        try {
            for (int i = 0; i < 3; i++) {
                splitAttrVal = "A";

                startGrid(2 * i);

                splitAttrVal = "B";

                startGrid(2 * i + 1);

                awaitPartitionMapExchange();

                checkPartitions();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkPartitions() throws Exception {
        AffinityFunction aff = cacheConfiguration(grid(0).configuration(), DEFAULT_CACHE_NAME).getAffinity();

        int partCnt = aff.partitions();

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < partCnt; i++) {
            Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(i);

            assertEquals(2, nodes.size());

            ClusterNode primary = F.first(nodes);
            ClusterNode backup = F.last(nodes);

            assertFalse(F.eq(primary.attribute(SPLIT_ATTRIBUTE_NAME), backup.attribute(SPLIT_ATTRIBUTE_NAME)));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionDistributionWithAffinityBackupFilter() throws Exception {
        backups = 3;

        try {
            for (int i = 0; i < 2; i++) {
                splitAttrVal = FIRST_NODE_GROUP;

                startGrid(4 * i);

                startGrid(4 * i + 3);

                splitAttrVal = "B";

                startGrid(4 * i + 1);

                splitAttrVal = "C";

                startGrid(4 * i + 2);

                awaitPartitionMapExchange();

                checkPartitionsWithAffinityBackupFilter();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkPartitionsWithAffinityBackupFilter() throws Exception {
        AffinityFunction aff = cacheConfiguration(grid(0).configuration(), DEFAULT_CACHE_NAME).getAffinity();

        int partCnt = aff.partitions();

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < partCnt; i++) {
            Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(i);

            assertEquals(backups + 1, nodes.size());

            Map<String, Integer> stat = getAttributeStatistic(nodes);

            assertEquals(stat.get(FIRST_NODE_GROUP), new Integer(2));

            assertEquals(stat.get("B"), new Integer(1));

            assertEquals(stat.get("C"), new Integer(1));
        }
    }
}