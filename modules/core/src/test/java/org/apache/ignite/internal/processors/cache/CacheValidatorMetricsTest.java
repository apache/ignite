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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Cache validator metrics test.
 */
public class CacheValidatorMetricsTest extends GridCommonAbstractTest implements Serializable {
    /** Cache name 1. */
    private static String CACHE_NAME_1 = "cache1";

    /** Cache name 2. */
    private static String CACHE_NAME_2 = "cache2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);

        CacheConfiguration cCfg1 = new CacheConfiguration()
            .setName(CACHE_NAME_1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName(CACHE_NAME_2)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setTopologyValidator(new TopologyValidator() {
            @Override public boolean validate(Collection<ClusterNode> nodes) {
                return nodes.size() == 2;
            }
        });

        cfg.setCacheConfiguration(cCfg1, cCfg2);

        return cfg;
    }

    /**
     * Asserts that the cache has appropriate status (indicated by the cache metrics).
     *
     * @param cacheName Cache name.
     * @param validForReading Cache is valid for reading.
     * @param validForWriting Cache is valid for writing.
     */
    void assertCacheStatus(String cacheName, boolean validForReading, boolean validForWriting) {
        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            assertEquals(validForReading, node.cache(cacheName).metrics().isValidForReading());
            assertEquals(validForWriting, node.cache(cacheName).metrics().isValidForWriting());
        }
    }

    /**
     * Test the cache validator metrics.
     * Cache can be invalid for writing due to invalid topology or due to partitions loss.
     * Now we can't reproduce test case with invalid for reading cache. At present, reading from the cache can be
     * invalid only for certain keys or when the cluster is not active (in this case caches are not available at all).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheValidatorMetrics() throws Exception {
        final IgniteEx crd = startGrid(1);
        crd.cluster().active(true);
        crd.cluster().baselineAutoAdjustEnabled(false);

        assertCacheStatus(CACHE_NAME_1, true, true);
        assertCacheStatus(CACHE_NAME_2, true, false);

        startGrid(2);
        resetBaselineTopology();

        awaitPartitionMapExchange();

        assertCacheStatus(CACHE_NAME_1, true, true);
        assertCacheStatus(CACHE_NAME_2, true, true);

        stopGrid(1);

        awaitPartitionMapExchange();

        // Invalid for writing due to partitions loss.
        assertCacheStatus(CACHE_NAME_1, false, false);

        // Invalid for writing due to invalid topology.
        assertCacheStatus(CACHE_NAME_2, true, false);
    }
}
