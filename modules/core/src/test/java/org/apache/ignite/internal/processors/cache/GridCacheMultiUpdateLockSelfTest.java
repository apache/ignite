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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.checkpoint.noop.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

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
        cfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);

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

            long topVer = cache.beginMultiUpdate();

            IgniteInternalFuture<?> startFut;

            try {
                assertEquals(3, topVer);

                final AtomicBoolean started = new AtomicBoolean();

                startFut = multithreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        info(">>>> Starting grid.");

                        Ignite g4 = startGrid(4);

                        started.set(true);

                        IgniteCache<Object, Object> c = g4.jcache(null);

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
                IgniteCache<Object, Object> cache0 = g.jcache(null);

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
