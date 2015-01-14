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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for reproduce problem with GG-6895:
 * putx calls CacheStore.load() when null GridPredicate passed in to avoid IDE warnings
 */
public class GridCacheStorePutxSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static AtomicInteger loads;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = new GridCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setAtomicityMode(TRANSACTIONAL);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setStore(new TestStore());

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        loads = new AtomicInteger();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxShouldNotTriggerLoad() throws Exception {
        assertTrue(cache().putx(1, 1));
        assertTrue(cache().putx(2, 2, (IgnitePredicate)null));

        assertEquals(0, loads.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxShouldNotTriggerLoadWithTx() throws Exception {
        GridCache<Integer, Integer> cache = cache();

        try (IgniteTx tx = cache.txStart()) {
            assertTrue(cache.putx(1, 1));
            assertTrue(cache.putx(2, 2, (IgnitePredicate)null));

            tx.commit();
        }

        assertEquals(0, loads.get());
    }

    /** */
    private static class TestStore implements GridCacheStore<Integer, Integer> {
        /** {@inheritDoc} */
        @Nullable @Override public Integer load(@Nullable IgniteTx tx, Integer key) throws IgniteCheckedException {
            loads.incrementAndGet();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, @Nullable Object... args)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void loadAll(@Nullable IgniteTx tx, Collection<? extends Integer> keys,
            IgniteBiInClosure<Integer, Integer> c) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable IgniteTx tx, Integer key,
            @Nullable Integer val) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void putAll(@Nullable IgniteTx tx,
            Map<? extends Integer, ? extends Integer> map) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable IgniteTx tx, Integer key)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeAll(@Nullable IgniteTx tx, Collection<? extends Integer> keys)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
            // No-op.
        }
    }
}
