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

package org.apache.ignite.cache;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.TestStorageUtils.corruptDataEntry;

/**
 * Tests check that unnecessary rebalances doesn't happen
 */
public class NoUnnecessaryRebalancesTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private static final int nodeCount = 3;

    /**
     * @return Grid test configuration.
     * @throws Exception If failed.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new SpecialSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(200 * 1024 * 1024)
        ));

        return cfg;
    }

    /**
     * Test check that cache creation doesn't invoke rebalance on cache in other cache group
     * @throws Exception If failed.
     */
    @Test
    public void testNoRebalancesOnCacheCreation() throws Exception {
        startGrids(nodeCount);

        Ignite g0 = grid(0);

        g0.cluster().state(ClusterState.ACTIVE);

        g0.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache0 = g0.cache(CACHE_NAME + 0);

        for (int i = 0; i < 100; i++)
            cache0.put(i, i);

        awaitPartitionMapExchange();

        GridCacheContext<Object, Object> cacheCtx0 = grid(0).cachex(CACHE_NAME + 0).context();

        corruptDataEntry(cacheCtx0, 1, true, false, new GridCacheVersion(0, 0, 0), "broken");

        g0.createCache(getCacheConfiguration(1));

        awaitPartitionMapExchange(true, true, null);

        Assert.assertFalse(SpecialSpi.rebGrpIds.contains(CU.cacheId(CACHE_NAME + 0)));
    }

    /** */
    private CacheConfiguration<Object, Object> getCacheConfiguration(int idx) {
        return new CacheConfiguration<>(CACHE_NAME + idx)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(8));
    }

    /**
     * Wrapper of communication spi to detect on which cache groups rebalances were happened.
     */
    public static class SpecialSpi extends TestRecordingCommunicationSpi {
        /** Cache groups on which rebalances were happened */
        public static final Set<Integer> rebGrpIds = new HashSet<>();

        /** Lock object. */
        private static final Object mux = new Object();

        /** */
        public static Set<Integer> allRebalances() {
            synchronized (mux) {
                return Collections.unmodifiableSet(rebGrpIds);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage supplyMsg = (GridDhtPartitionSupplyMessage) ((GridIoMessage)msg).message();

                synchronized (mux) {
                    rebGrpIds.add(supplyMsg.groupId());
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }
}
