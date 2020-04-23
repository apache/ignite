/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsExchangeDuringCheckpointTest extends GridCommonAbstractTest {
    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /**
     *
     */
    @Test
    public void testExchangeOnNodeLeft() throws Exception {
        for (int i = 0; i < SF.applyLB(5, 2); i++) {
            startGrids(3);
            IgniteEx ignite = grid(1);
            ignite.active(true);

            awaitPartitionMapExchange();

            stopGrid(0, true);

            awaitPartitionMapExchange();

            ignite.context().cache().context().database().wakeupForCheckpoint("test").get(10000);

            afterTest();
        }
    }

    /**
     *
     */
    @Test
    public void testExchangeOnNodeJoin() throws Exception {
        for (int i = 0; i < SF.applyLB(5, 2); i++) {
            startGrids(2);
            IgniteEx ignite = grid(1);
            ignite.active(true);

            awaitPartitionMapExchange();

            IgniteEx ex = startGrid(2);

            awaitPartitionMapExchange();

            ex.context().cache().context().database().wakeupForCheckpoint("test").get(10000);

            afterTest();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(800L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointThreads(1)
            .setCheckpointFrequency(1);

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(200L * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        CacheConfiguration ccfgNp = new CacheConfiguration("nonPersistentCache");
        ccfgNp.setDataRegionName(NO_PERSISTENCE_REGION);
        ccfgNp.setDiskPageCompression(null);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 4096));

        cfg.setCacheConfiguration(ccfg, ccfgNp);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }
}
