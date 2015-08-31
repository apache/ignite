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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests multi-update locks.
 */
public class GridCacheMultiUpdateLockSelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setCheckpointSpi(new NoopCheckpointSpi());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);

        if (!nearEnabled)
            cfg.setNearConfiguration(null);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setRebalanceMode(SYNC);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiUpdateLocksNear() throws Exception {
        checkMultiUpdateLocks(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiUpdateLocksColocated() throws Exception {
        checkMultiUpdateLocks(false);
    }

    /**
     * @param nearEnabled Near enabled flag.
     * @throws Exception If failed.
     */
    private void checkMultiUpdateLocks(boolean nearEnabled) throws Exception {
        this.nearEnabled = nearEnabled;

        startGrids(3);

        try {
            IgniteKernal g = (IgniteKernal)grid(0);

            GridCacheContext<Object, Object> cctx = g.internalCache().context();

            GridDhtCacheAdapter cache = nearEnabled ? cctx.near().dht() : cctx.colocated();

            AffinityTopologyVersion topVer = cache.beginMultiUpdate();

            IgniteInternalFuture<?> startFut;

            try {
                assertEquals(3, topVer.topologyVersion());

                final AtomicBoolean started = new AtomicBoolean();

                startFut = multithreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        info(">>>> Starting grid.");

                        Ignite g4 = startGrid(4);

                        started.set(true);

                        IgniteCache<Object, Object> c = g4.cache(null);

                        info(">>>> Checking tx in new grid.");

                        try (Transaction tx = g4.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            assertEquals(2, c.get("a"));
                            assertEquals(4, c.get("b"));
                            assertEquals(6, c.get("c"));
                        }

                        return null;
                    }
                }, 1);

                U.sleep(200);

                info(">>>> Checking grid has not started yet.");

                assertFalse(started.get());

                // Check we can proceed with transactions.
                IgniteCache<Object, Object> cache0 = g.cache(null);

                info(">>>> Checking tx commit.");

                Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    cache0.put("a", 1);
                    cache0.put("b", 2);
                    cache0.put("c", 3);

                    tx.commit();
                }
                finally {
                    tx.close();
                }

                info(">>>> Checking grid still is not started");

                assertFalse(started.get());

                tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    cache0.put("a", 2);
                    cache0.put("b", 4);
                    cache0.put("c", 6);

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }
            finally {
                info(">>>> Releasing multi update.");

                cache.endMultiUpdate();
            }

            info("Waiting for thread termination.");

            startFut.get();
        }
        finally {
            stopAllGrids();
        }
    }
}