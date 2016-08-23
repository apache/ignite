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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheMessageRecoveryIdleConnection extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 3;

    /** */
    private static final long IDLE_TIMEOUT = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setIdleConnectionTimeout(IDLE_TIMEOUT);
        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperationsIdleConnectionCloseTx() throws Exception {
        cacheOperationsIdleConnectionClose(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperationsIdleConnectionCloseAtomic() throws Exception {
        cacheOperationsIdleConnectionClose(ATOMIC);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void cacheOperationsIdleConnectionClose(CacheAtomicityMode atomicityMode) throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Object, Object> cache = ignite(0).createCache(ccfg).withAsync();

        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int iter = 0;

            long stopTime = System.currentTimeMillis() + 90_000;

            while (System.currentTimeMillis() < stopTime) {
                if (iter++ % 10 == 0)
                    log.info("Iteration: " + iter);

                cache.put(iter, 1);

                IgniteFuture<?> fut = cache.future();

                try {
                    fut.get(10_000);
                }
                catch (IgniteException e) {
                    List<Ignite> nodes = IgnitionEx.allGridsx();

                    for (Ignite node : nodes)
                        ((IgniteKernal)node).dumpDebugInfo();

                    U.dumpThreads(log);

                    throw e;
                }

                U.sleep(rnd.nextLong(IDLE_TIMEOUT - 10, IDLE_TIMEOUT + 10));
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }
}
