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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class SemaphoreFailoverSafeReleasePermitsTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Atomics cache mode. */
    private CacheMode atomicsCacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        AtomicConfiguration atomicCfg = atomicConfiguration();

        assertNotNull(atomicCfg);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePermitsPartitioned() throws Exception {
        atomicsCacheMode = PARTITIONED;

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePermitsReplicated() throws Exception {
        atomicsCacheMode = REPLICATED;

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        try {
            startGrids(GRID_CNT);

            Ignite ignite = grid(0);

            IgniteSemaphore sem = ignite.semaphore("sem", 1, true, true);

            assertEquals(1, sem.availablePermits());

            sem.acquire(1);

            assertEquals(0, sem.availablePermits());

            ignite.close();

            awaitPartitionMapExchange();

            ignite = grid(1);

            sem = ignite.semaphore("sem", 1, true, true);

            assertTrue(sem.tryAcquire(1, 5000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return Atomic configuration.
     */
    protected AtomicConfiguration atomicConfiguration() {
        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(atomicsCacheMode);

        if (atomicsCacheMode == PARTITIONED)
            atomicCfg.setBackups(1);

        return atomicCfg;
    }
}
