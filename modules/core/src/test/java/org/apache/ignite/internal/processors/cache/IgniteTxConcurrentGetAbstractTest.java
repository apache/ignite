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

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Checks multithreaded put/get cache operations on one node.
 */
public abstract class IgniteTxConcurrentGetAbstractTest extends GridCommonAbstractTest {
    /** Debug flag. */
    private static final boolean DEBUG = false;

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int THREAD_NUM = 20;

    /**
     * Default constructor.
     *
     */
    protected IgniteTxConcurrentGetAbstractTest() {
        super(true /** Start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * @param g Grid.
     * @return Near cache.
     */
    GridNearCacheAdapter<String, Integer> near(Ignite g) {
        return (GridNearCacheAdapter<String, Integer>)((IgniteKernal)g).<String, Integer>internalCache();
    }

    /**
     * @param g Grid.
     * @return DHT cache.
     */
    GridDhtCacheAdapter<String, Integer> dht(Ignite g) {
        return near(g).dht();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutGet() throws Exception {
        // Random key.
        final String key = UUID.randomUUID().toString();

        final Ignite ignite = grid();

        ignite.cache(null).put(key, "val");

        GridCacheEntryEx dhtEntry = dht(ignite).peekEx(key);

        if (DEBUG)
            info("DHT entry [hash=" + System.identityHashCode(dhtEntry) + ", entry=" + dhtEntry + ']');

        String val = txGet(ignite, key);

        assertNotNull(val);

        info("Starting threads: " + THREAD_NUM);

        multithreaded(new Callable<String>() {
            @Override public String call() throws Exception {
                return txGet(ignite, key);
            }
        }, THREAD_NUM, "getter-thread");
    }

    /**
     * @param ignite Grid.
     * @param key Key.
     * @return Value.
     * @throws Exception If failed.
     */
    private String txGet(Ignite ignite, String key) throws Exception {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            GridCacheEntryEx dhtEntry = dht(ignite).peekEx(key);

            if (DEBUG)
                info("DHT entry [hash=" + System.identityHashCode(dhtEntry) + ", xid=" + tx.xid() +
                    ", entry=" + dhtEntry + ']');

            String val = ignite.<String, String>cache(null).get(key);

            assertNotNull(val);
            assertEquals("val", val);

            tx.commit();

            return val;
        }
    }
}