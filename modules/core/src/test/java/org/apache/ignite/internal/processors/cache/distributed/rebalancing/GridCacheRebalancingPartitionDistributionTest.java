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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.assertions.Assertion;
import org.apache.ignite.testframework.junits.common.GridRollingRestartAbstractTest;
import org.junit.Test;

/**
 * Test the behavior of the partition rebalancing during a rolling restart.
 */
public class GridCacheRebalancingPartitionDistributionTest extends GridRollingRestartAbstractTest {
    /** The maximum allowable deviation from a perfect distribution. */
    private static final double MAX_DEVIATION = 0.20;

    /** Test cache name. */
    private static final String CACHE_NAME = "PARTITION_DISTRIBUTION_TEST";

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Integer> getCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(true /* machine-safe */, 1024))
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /**
     * The test performs rolling restart and checks no server drops out and the partitions are balanced during
     * redistribution.
     */
    @Test
    public void testRollingRestart() throws InterruptedException {
        awaitPartitionMapExchange();

        rollingRestartThread.join();

        assertEquals(getMaxRestarts(), rollingRestartThread.getRestartTotal());
    }

    /** {@inheritDoc} */
    @Override public int serverCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRestarts() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<Ignite> getRestartCheck() {
        return new IgnitePredicate<Ignite>() {
            @Override public boolean apply(final Ignite ignite) {
                Collection<ClusterNode> srvs = ignite.cluster().forServers().nodes();

                if (srvs.size() < serverCount())
                    return false;

                for (ClusterNode node : srvs) {
                    int[] primaries = ignite.affinity(CACHE_NAME).primaryPartitions(node);

                    if (primaries == null || primaries.length == 0)
                        return false;
                }

                return true;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Assertion getRestartAssertion() {
        return new FairDistributionAssertion();
    }

    /**
     * Assertion for {@link RollingRestartThread} to perform prior to each restart to test
     * the Partition Distribution.
     */
    private class FairDistributionAssertion extends CacheNodeSafeAssertion {
        /** Construct a new FairDistributionAssertion. */
        public FairDistributionAssertion() {
            super(grid(0), CACHE_NAME);
        }

        /** {@inheritDoc} */
        @Override public void test() throws AssertionError {
            super.test();

            Affinity<?> affinity = ignite().affinity(CACHE_NAME);

            int partCnt = affinity.partitions();

            Map<ClusterNode, Integer> partMap = new HashMap<>(serverCount());

            for (int i = 0; i < partCnt; i++) {
                ClusterNode node = affinity.mapPartitionToNode(i);

                int cnt = partMap.containsKey(node) ? partMap.get(node) : 0;

                partMap.put(node, cnt + 1);
            }

            int fairCnt = partCnt / serverCount();

            for (int count : partMap.values()) {
                double deviation = Math.abs(fairCnt - count) / (double)fairCnt;

                if (deviation > MAX_DEVIATION) {
                    throw new AssertionError("partition distribution deviation exceeded max: fair count=" + fairCnt
                            + ", actual count=" + count + ", deviation=" + deviation);
                }
            }
        }
    }
}
