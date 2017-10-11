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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgnitePdsExchangeDuringCheckpointTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public void testExchangeOnNodeLeft() throws Exception {
        for (int i = 0; i < 5; i++) {
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
    public void testExchangeOnNodeJoin() throws Exception {
        for (int i = 0; i < 5; i++) {
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

    /**

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(100 * 1024 * 1024);
        memPlcCfg.setMaxSize(1000 * 1024 * 1024);

        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");
        memCfg.setMemoryPolicies(memPlcCfg);

        cfg.setMemoryConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 4096));

        cfg.setCacheConfiguration(ccfg);

        PersistentStoreConfiguration psiCfg = new PersistentStoreConfiguration()
            .setCheckpointingThreads(1)
            .setCheckpointingFrequency(1)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setPersistentStoreConfiguration(psiCfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
