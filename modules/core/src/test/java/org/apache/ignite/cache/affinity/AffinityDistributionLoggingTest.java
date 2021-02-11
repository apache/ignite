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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Tests of partitions distribution logging.
 *
 * Tests based on using of affinity function which provides an even distribution of partitions between nodes.
 *
 * @see EvenDistributionAffinityFunction
 */
public class AffinityDistributionLoggingTest extends GridCommonAbstractTest {
    /** Pattern to test. */
    private static final String LOG_MESSAGE_PREFIX = "Local node affinity assignment distribution is not ideal ";

    /** Partitions number. */
    private int parts = 0;

    /** Nodes number. */
    private int nodes = 0;

    /** Backups number. */
    private int backups = 0;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(backups);
        cacheCfg.setAffinity(new EvenDistributionAffinityFunction(parts));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, value = "0")
    public void test2PartitionsIdealDistributionIsNotLogged() throws Exception {
        nodes = 2;
        parts = 2;
        backups = 1;

        String testsLog = runAndGetExchangeLog(false);

        assertFalse(testsLog.contains(LOG_MESSAGE_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, value = "0.0")
    public void test120PartitionsIdeadDistributionIsNotLogged() throws Exception {
        nodes = 3;
        parts = 120;
        backups = 2;

        String testsLog = runAndGetExchangeLog(false);

        assertFalse(testsLog.contains(LOG_MESSAGE_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, value = "50.0")
    public void test5PartitionsNotIdealDistributionIsLogged() throws Exception {
        nodes = 4;
        parts = 5;
        backups = 3;

        String testsLog = runAndGetExchangeLog(false);

        assertTrue(testsLog.contains(LOG_MESSAGE_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, value = "0.0")
    public void test5PartitionsNotIdealDistributionSuppressedLoggingOnClientNode() throws Exception {
        nodes = 4;
        parts = 5;
        backups = 3;

        String testsLog = runAndGetExchangeLog(true);

        assertFalse(testsLog.contains(LOG_MESSAGE_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, value = "50.0")
    public void test7PartitionsNotIdealDistributionSuppressedLogging() throws Exception {
        nodes = 3;
        parts = 7;
        backups = 0;

        String testsLog = runAndGetExchangeLog(false);

        assertFalse(testsLog.contains(LOG_MESSAGE_PREFIX));
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, value = "65")
    public void test5PartitionsNotIdealDistributionSuppressedLogging() throws Exception {
        nodes = 4;
        parts = 5;
        backups = 3;

        String testsLog = runAndGetExchangeLog(false);

        assertFalse(testsLog.contains(LOG_MESSAGE_PREFIX));
    }

    /**
     * Starts a specified number of Ignite nodes and log partition node exchange during a last node's startup.
     *
     * @param testClientNode Whether it is necessary to get exchange log from the client node.
     * @return Log of latest partition map exchange.
     * @throws Exception In case of an error.
     */
    private String runAndGetExchangeLog(boolean testClientNode) throws Exception {
        assert nodes > 1;

        IgniteEx ignite = startGrids(nodes - 1);

        awaitPartitionMapExchange();

        GridCacheProcessor proc = ignite.context().cache();

        GridCacheContext cctx = proc.context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME));

        final GridStringLogger log = new GridStringLogger(false, this.log);

        GridAffinityAssignmentCache aff = GridTestUtils.getFieldValue(cctx.affinity(), "aff");

        GridTestUtils.setFieldValue(aff, "log", log);

        if (testClientNode)
            startClientGrid(getConfiguration("client"));
        else
            startGrid(nodes);

        awaitPartitionMapExchange();

        return log.toString();
    }

    /**
     * Affinity function for a partitioned cache which provides even distribution partitions between nodes in cluster.
     */
    private static class EvenDistributionAffinityFunction implements AffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** Partitions number. */
        private int parts;

        /**
         * @param parts Number of partitions for one cache.
         */
        private EvenDistributionAffinityFunction(int parts) {
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
            List<ClusterNode> nodes = new ArrayList<>(affCtx.currentTopologySnapshot());

            nodes.sort(Comparator.comparing(o -> o.<String>attribute(ATTR_IGNITE_INSTANCE_NAME)));

            List<List<ClusterNode>> res = new ArrayList<>(parts);

            for (int i = 0; i < parts; i++) {
                Set<ClusterNode> n0 = new LinkedHashSet<>();

                n0.add(nodes.get(i % nodes.size()));

                for (int j = 1; j <= affCtx.backups(); j++)
                    n0.add(nodes.get((i + j) % nodes.size()));

                res.add(new ArrayList<>(n0));
            }

            return res;
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
