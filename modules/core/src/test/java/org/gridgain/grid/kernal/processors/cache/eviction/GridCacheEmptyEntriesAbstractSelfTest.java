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

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.cache.eviction.GridCacheEvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.GridCacheFifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.*;
import javax.cache.configuration.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;

/**
 * Tests that cache handles {@code setAllowEmptyEntries} flag correctly.
 */
public abstract class GridCacheEmptyEntriesAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private GridCacheEvictionPolicy<?, ?> plc;

    /** */
    private GridCacheEvictionPolicy<?, ?> nearPlc;

    /** Test store. */
    private CacheStore<String, String> testStore;

    /** Tx concurrency to use. */
    private IgniteTxConcurrency txConcurrency;

    /** Tx isolation to use. */
    private IgniteTxIsolation txIsolation;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TransactionsConfiguration txCfg = c.getTransactionsConfiguration();

        txCfg.setDefaultTxConcurrency(txConcurrency);
        txCfg.setDefaultTxIsolation(txIsolation);
        txCfg.setTxSerializableEnabled(true);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

        cc.setEvictionPolicy(plc);
        cc.setNearEvictionPolicy(nearPlc);
        cc.setEvictSynchronizedKeyBufferSize(1);

        cc.setEvictNearSynchronized(true);
        cc.setEvictSynchronized(true);

        if (testStore != null) {
            cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(testStore));
            cc.setReadThrough(true);
            cc.setWriteThrough(true);
            cc.setLoadPreviousValue(true);
        }
        else
            cc.setCacheStoreFactory(null);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * Starts grids depending on testing cache.
     *
     * @return First grid node.
     * @throws Exception If failed.
     */
    protected abstract Ignite startGrids() throws Exception;

    /** @return Cache mode for particular test. */
    protected abstract GridCacheMode cacheMode();

    /**
     * Tests FIFO eviction policy.
     *
     * @throws Exception If failed.
     */
    public void testFifo() throws Exception {
        plc = new GridCacheFifoEvictionPolicy(50);
        nearPlc = new GridCacheFifoEvictionPolicy(50);

        checkPolicy();
    }

    /**
     * Checks policy with and without store set.
     *
     * @throws Exception If failed.
     */
    private void checkPolicy() throws Exception {
        testStore = null;

        checkPolicy0();

        testStore = new CacheStoreAdapter<String, String>() {
            @Override public String load(String key) {
                return null;
            }

            @Override public void write(Cache.Entry<? extends String, ? extends String> e) {
                // No-op.
            }

            @Override public void delete(Object key) {
                // No-op.
            }
        };

        checkPolicy0();
    }

    /**
     * Tests preset eviction policy.
     *
     * @throws Exception If failed.
     */
    private void checkPolicy0() throws Exception {
        for (IgniteTxConcurrency concurrency : IgniteTxConcurrency.values()) {
            txConcurrency = concurrency;

            for (IgniteTxIsolation isolation : IgniteTxIsolation.values()) {
                txIsolation = isolation;

                Ignite g = startGrids();

                GridCache<String, String> cache = g.cache(null);

                try {
                    info(">>> Checking policy [txConcurrency=" + txConcurrency + ", txIsolation=" + txIsolation +
                        ", plc=" + plc + ", nearPlc=" + nearPlc + ']');

                    checkExplicitTx(cache);

                    checkImplicitTx(cache);
                }
                finally {
                    stopAllGrids();
                }
            }
        }
    }

    /**
     * Checks that gets work for implicit txs.
     *
     * @param cache Cache to test.
     * @throws Exception If failed.
     */
    private void checkImplicitTx(GridCache<String, String> cache) throws Exception {
        assertNull(cache.get("key1"));
        assertNull(cache.getAsync("key2").get());

        assertTrue(cache.getAll(F.asList("key3", "key4")).isEmpty());
        assertTrue(cache.getAllAsync(F.asList("key5", "key6")).get().isEmpty());

        cache.put("key7", "key7");
        cache.remove("key7", "key7");
        assertNull(cache.get("key7"));

        checkEmpty(cache);
    }

    /**
     * Checks that gets work for implicit txs.
     *
     * @param cache Cache to test.
     * @throws Exception If failed.
     */
    private void checkExplicitTx(GridCache<String, String> cache) throws Exception {
        IgniteTx tx = cache.txStart();

        try {
            assertNull(cache.get("key1"));

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            assertNull(cache.getAsync("key2").get());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            assertTrue(cache.getAll(F.asList("key3", "key4")).isEmpty());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            assertTrue(cache.getAllAsync(F.asList("key5", "key6")).get().isEmpty());

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart();

        try {
            cache.put("key7", "key7");

            cache.remove("key7");

            assertNull(cache.get("key7"));

            tx.commit();
        }
        finally {
            tx.close();
        }

        checkEmpty(cache);
    }

    /**
     * Checks that cache is empty.
     *
     * @param cache Cache to check.
     * @throws org.apache.ignite.IgniteInterruptedException If interrupted while sleeping.
     */
    @SuppressWarnings({"ErrorNotRethrown", "TypeMayBeWeakened"})
    private void checkEmpty(GridCache<String, String> cache) throws IgniteInterruptedException {
        for (int i = 0; i < 3; i++) {
            try {
                assertTrue(cache.entrySet().toString(), cache.entrySet().isEmpty());

                break;
            }
            catch (AssertionError e) {
                if (i == 2)
                    throw e;

                info(">>> Cache is not empty, flushing evictions.");

                U.sleep(1000);
            }
        }
    }
}
