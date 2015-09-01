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

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.PE;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 */
public class GridCacheMarshallingNodeJoinSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, TestObject> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setCacheStoreFactory(new StoreFactory());
        cacheCfg.setReadThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoin() throws Exception {
        final CountDownLatch allowJoin = new CountDownLatch(1);
        final CountDownLatch joined = new CountDownLatch(2);

        for (int i = 0; i < 2; i++) {
            ignite(i).events().localListen(new PE() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EventType.EVT_NODE_JOINED;

                    info(">>> Event: " + evt);

                    joined.countDown();

                    return true;
                }
            }, EventType.EVT_NODE_JOINED);
        }

        IgniteInternalFuture<?> oneMoreGrid = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                allowJoin.await();

                startGrid("oneMoreGrid");

                return null;
            }
        }, 1);

        IgniteCache<Integer, TestObject> cache = ignite(0).cache(null);

        try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.get(0);

            allowJoin.countDown();

            joined.await();

            assertNotNull(cache.get(1));

            tx.commit();
        }

        oneMoreGrid.get();

        assertNotNull(cache.get(1));
    }

    /**
     */
    private static class StoreFactory implements Factory<CacheStore<? super Integer, ? super TestObject>> {
        /** {@inheritDoc} */
        @Override public CacheStore<? super Integer, ? super TestObject> create() {
            return new Store();
        }
    }

    /**
     */
    private static class Store extends CacheStoreAdapter<Integer, TestObject> implements Serializable {
        /** {@inheritDoc} */
        @Override public TestObject load(Integer key) throws CacheLoaderException {
            return key > 0 ? new TestObject() : null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends TestObject> e)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }

    /**
     */
    private static class TestObject implements Serializable {
        /**
         */
        public TestObject() {
            // No-op.
        }
    }
}