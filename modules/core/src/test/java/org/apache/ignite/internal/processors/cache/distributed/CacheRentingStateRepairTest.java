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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheRentingStateRepairTest extends GridCommonAbstractTest {
    /** */
    private boolean persistenceEnabled;

    public static final int PARTS = 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS).setPartitions(64));

        ccfg.setOnheapCacheEnabled(false);

        ccfg.setBackups(1);

        ccfg.setRebalanceBatchSize(100);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        long sz = 100 * 1024 * 1024;

        if (persistenceEnabled) {
            DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(sz).setMaxSize(sz))
                .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

            cfg.setDataStorageConfiguration(memCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName(int idx) {
        return "node" + idx;
    }

    /**
     *
     */
    public void testRentingStateRepairWithPersistence() throws Exception {
        persistenceEnabled = true;

        try {
            IgniteEx g0 = startGrid(0);

            startGrid(1);

            g0.cluster().active(true);

            awaitPartitionMapExchange();

            int toEvictPart = 12;

            int k = 0;

            while(true) {
                if (g0.affinity(DEFAULT_CACHE_NAME).partition(k) == toEvictPart)
                    break;

                k++;
            }

            g0.cache(DEFAULT_CACHE_NAME).put(k, k);

            GridDhtLocalPartition part = dht(g0.cache(DEFAULT_CACHE_NAME)).topology().localPartition(toEvictPart);

            assertNotNull(part);

            IgniteEx g2 = startGrid(2);

            part.debugPreventClear = true;

            g0.cluster().setBaselineTopology(3);

            awaitPartitionMapExchange();

            assertEquals(GridDhtPartitionState.RENTING, part.state());

            stopGrid(0);

            g0 = startGrid(0);

            awaitPartitionMapExchange();

            doSleep(5_000);

            part = dht(g0.cache(DEFAULT_CACHE_NAME)).topology().localPartition(toEvictPart);

            assertNotNull(part);

            final GridDhtLocalPartition finalPart = part;

            part.onClearFinished(new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> future) {
                    assertEquals(GridDhtPartitionState.EVICTED, finalPart.state());
                }
            });
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        U.IGNITE_TEST_FEATURES_ENABLED = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();

        U.IGNITE_TEST_FEATURES_ENABLED = false;
    }
}
