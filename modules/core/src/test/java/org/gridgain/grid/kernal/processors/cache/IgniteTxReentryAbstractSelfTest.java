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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.direct.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Tests reentry in pessimistic repeatable read tx.
 */
public abstract class IgniteTxReentryAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** @return Cache mode. */
    protected abstract GridCacheMode cacheMode();

    /** @return Near enabled. */
    protected abstract boolean nearEnabled();

    /** @return Grid count. */
    protected abstract int gridCount();

    /** @return Test key. */
    protected abstract int testKey();

    /** @return Expected number of near lock requests. */
    protected abstract int expectedNearLockRequests();

    /** @return Expected number of near lock requests. */
    protected abstract int expectedDhtLockRequests();

    /** @return Expected number of near lock requests. */
    protected abstract int expectedDistributedLockRequests();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new CountingCommunicationSpi());
        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(nearEnabled() ? NEAR_PARTITIONED : PARTITIONED_ONLY);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testLockReentry() throws Exception {
        startGrids(gridCount());

        try {
            GridCache<Object, Object> cache = grid(0).cache(null);

            // Find test key.
            int key = testKey();

            try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // One near lock request.
                cache.get(key);

                // No more requests.
                cache.remove(key);

                tx.commit();
            }

            CountingCommunicationSpi commSpi = (CountingCommunicationSpi)grid(0).configuration().getCommunicationSpi();

            assertEquals(expectedNearLockRequests(), commSpi.nearLocks());
            assertEquals(expectedDhtLockRequests(), commSpi.dhtLocks());
            assertEquals(expectedDistributedLockRequests(), commSpi.distributedLocks());
        }
        finally {
            stopAllGrids();
        }
    }

    /** Counting communication SPI. */
    protected static class CountingCommunicationSpi extends TcpCommunicationSpi {
        /** Distributed lock requests. */
        private AtomicInteger distLocks = new AtomicInteger();

        /** Near lock requests. */
        private AtomicInteger nearLocks = new AtomicInteger();

        /** Dht locks. */
        private AtomicInteger dhtLocks = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
            throws IgniteSpiException {
            countMsg((GridIoMessage)msg);

            super.sendMessage(node, msg);
        }

        /**
         * Unmarshals the message and increments counters.
         *
         * @param msg Message to check.
         */
        private void countMsg(GridIoMessage msg) {
            Object origMsg = msg.message();

            if (origMsg instanceof GridDistributedLockRequest) {
                distLocks.incrementAndGet();

                if (origMsg instanceof GridNearLockRequest)
                    nearLocks.incrementAndGet();
                else if (origMsg instanceof GridDhtLockRequest)
                    dhtLocks.incrementAndGet();
            }
        }

        /** @return Number of recorded distributed locks. */
        public int distributedLocks() {
            return distLocks.get();
        }

        /** @return Number of recorded distributed locks. */
        public int nearLocks() {
            return nearLocks.get();
        }

        /** @return Number of recorded distributed locks. */
        public int dhtLocks() {
            return dhtLocks.get();
        }
    }
}
