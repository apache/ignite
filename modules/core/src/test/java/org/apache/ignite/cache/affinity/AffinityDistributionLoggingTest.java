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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Tests of partitions distribution logging.
 */
public class AffinityDistributionLoggingTest extends GridCommonAbstractTest {
    /** Pattern to test. */
    private static final String CHECK_MSG_PREFIX = "Local node affinity assignment distribution is not ideal ";

    /** Partitions number. */
    private int parts = 0;

    /** Backups number. */
    private int backups = 0;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD);

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(backups);
        cacheCfg.setAffinity(new TestAffinityFunction(parts));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testIdealPartitionDistributionLogging1() throws Exception {
        System.setProperty(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, "0.0");

        String testsLog = runAndGetExchangeLog(2, 1, 2);

        assertFalse(testsLog.contains(CHECK_MSG_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testIdealPartitionDistributionLogging2() throws Exception {
        System.setProperty(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, "0.0");

        String testsLog = runAndGetExchangeLog(120, 2, 3);

        assertFalse(testsLog.contains(CHECK_MSG_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testNotIdealPartitionDistributionLogging1() throws Exception {
        String testsLog = runAndGetExchangeLog(4, 4, 4);

        String exp = CHECK_MSG_PREFIX + "[cache=default, expectedPrimary=1,00(25,00%), expectedBackups=4,00(100,00%), " +
            "primary=1(25,00%), backups=3(75,00%)]";

        assertTrue(testsLog.contains(exp));
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testNotIdealPartitionDistributionSuppressedLogging1() throws Exception {
        System.setProperty(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, "0.25");

        String testsLog = runAndGetExchangeLog(4, 4, 4);

        assertFalse(testsLog.contains(CHECK_MSG_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testNotIdealPartitionDistributionLogging2() throws Exception {
        System.setProperty(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, "0.65");

        String testsLog = runAndGetExchangeLog(39, 6, 3);

        String exp = CHECK_MSG_PREFIX + "[cache=default, expectedPrimary=13,00(33,33%), expectedBackups=78,00(200,00%), " +
            "primary=13(33,33%), backups=26(66,67%)]";

        assertTrue(testsLog.contains(exp));
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testNotIdealPartitionDistributionSuppressedLogging2() throws Exception {
        System.setProperty(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, "0.67");

        String testsLog = runAndGetExchangeLog(39, 6, 3);

        assertFalse(testsLog.contains(CHECK_MSG_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    private String runAndGetExchangeLog(int parts, int backups, int nodes) throws Exception {
        assert nodes > 1;

        this.parts = parts;
        this.backups = backups;

        IgniteEx ignite = (IgniteEx)startGrids(nodes - 1);

        awaitPartitionMapExchange();

        GridCacheProcessor proc = ignite.context().cache();

        GridCacheContext cctx = proc.context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME));

        final GridStringLogger log = new GridStringLogger(false, this.log);

        GridAffinityAssignmentCache aff = GridTestUtils.getFieldValue(cctx.affinity(), "aff");

        GridTestUtils.setFieldValue(aff, "log", log);

        startGrid(nodes);

        awaitPartitionMapExchange();

        X.println("***" + log.toString());

        return log.toString();
    }

    /**
     * Test affinity function.
     */
    private static class TestAffinityFunction implements AffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** Partitions number. */
        private int parts;

        /**
         * @param parts Number of partitions for one cache.
         */
        TestAffinityFunction(int parts) {
            this.parts = parts;
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return parts;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return key.hashCode() % parts;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> sortedNodes = sortNodes(affCtx.currentTopologySnapshot());

            List<List<ClusterNode>> res = new ArrayList<>(parts);

            int backups = Math.min(affCtx.backups(), sortedNodes.size() - 1);

            for (int i = 0; i < parts; i++) {
                List<ClusterNode> nodes = new ArrayList<>(backups + 1);

                nodes.add(mapPartitionToNode(i, sortedNodes));

                for (int j = 1; j <= backups; j++)
                    nodes.add(mapPartitionToNode(i + j, sortedNodes));

                res.add(nodes);
            }

            return res;
        }

        /**
         * @param part Partition number.
         * @param sortedNodes Nodes sorted by name.
         * @return Mapped node.
         */
        private ClusterNode mapPartitionToNode(int part, List<ClusterNode> sortedNodes) {
            if (part < sortedNodes.size())
                return sortedNodes.get(part);

            return sortedNodes.get(part % sortedNodes.size());
        }

        /**
         * @param clusterNodes Nodes for sorting.
         * @return New list which contains sorted nodes by node name.
         */
        private List<ClusterNode> sortNodes(List<ClusterNode> clusterNodes) {
            List<ClusterNode> nodes = new ArrayList<>(clusterNodes);

            nodes.sort(Comparator.comparing(o -> o.<String>attribute(ATTR_IGNITE_INSTANCE_NAME)));

            return nodes;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }
    }
}